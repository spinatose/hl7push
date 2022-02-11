package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"go.uber.org/ratelimit"
	"golang.org/x/oauth2"
	goauth2 "golang.org/x/oauth2/google"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const (
	scope           = "https://www.googleapis.com/auth/cloud-healthcare"
	projectsPath    = "projects"
	locationsPath   = "locations"
	datasetsPath    = "datasets"
	hl7storePath    = "hl7V2Stores"
	apiFormatHeader = "X-GOOG-API-FORMAT-VERSION"
)

type Config struct {
	Credential string `json:"credential" config:"credential"`
	ProjectID  string `json:"project" config:"project,description=GCP Project ID"`
	LocationID string `json:"location" config:"location,description=GCP Location"`
	DatasetID  string `json:"dataset" config:"dataset,description=HC API Dataset"`
	HL7StoreID string `json:"store" config:"store,description=HC API HL7 Store"`
	RateLimit  int    `json:"rate_limit" config:"rate_limit"`
}

func (c Config) Validate() error {
	fmt.Println("validating config for hcapi client...")

	if c.ProjectID == "" {
		return errors.New("missing project id")
	}

	if c.LocationID == "" {
		return errors.New("missing location id")
	}

	if c.DatasetID == "" {
		return errors.New("missing dataset id")
	}

	if c.HL7StoreID == "" {
		return errors.New("missing hl7 store id")
	}

	return nil
}

type Client interface {
	Send(data []byte) ([]byte, string, error)
	GetByID(id string) (*healthcare.Message, error)
	Get(path string) (*healthcare.Message, error)
	List() (*healthcare.ListMessagesResponse, error)
}

type client struct {
	config  Config
	store   *healthcare.ProjectsLocationsDatasetsHl7V2StoresService
	limiter ratelimit.Limiter
}

func NewClient(ctx context.Context, config Config) (Client, error) {
	fmt.Println("new hcapi client instantiation...")
	if err := config.Validate(); err != nil {
		return nil, err
	}

	store, err := initHL7Store(ctx, config.Credential)
	if err != nil {
		return nil, err
	}

	var rl ratelimit.Limiter
	if config.RateLimit > 0 {
		rl = ratelimit.New(config.RateLimit)
	} else {
		rl = ratelimit.NewUnlimited()
	}

	c := &client{
		store:   store,
		config:  config,
		limiter: rl,
	}

	fmt.Println("new hcapi client created")
	return c, nil
}

func (c *client) Send(data []byte) ([]byte, string, error) {
	resultpath := ""
	c.limiter.Take()

	fmt.Println("sending message on hcapi client")

	req := &healthcare.IngestMessageRequest{
		Message: &healthcare.Message{
			Labels: map[string]string{},
			Data:   base64.StdEncoding.EncodeToString(data),
		},
	}

	ctx := context.Background()
	storeName := genStoreName(c.config)
	ingest := c.store.Messages.Ingest(storeName, req)
	ingest.Header().Add(apiFormatHeader, "2")

	resp, err := ingest.Context(ctx).Do()
	if err != nil {
		return nil, resultpath, err
	}

	fmt.Println("wrote message to hcapi")
	resultpath = resp.Message.Name

	res, err := base64.StdEncoding.DecodeString(resp.Hl7Ack)
	if err != nil {
		return nil, "", err
	}

	return res, resultpath, nil
}

func (c *client) List() (*healthcare.ListMessagesResponse, error) {
	c.limiter.Take()
	fmt.Println("list messages on hcapi client")

	ctx := context.Background()
	storeName := genStoreName(c.config)

	list := c.store.Messages.List(storeName)
	list.Header().Add(apiFormatHeader, "2")

	return list.Context(ctx).Do()
}

func (c *client) GetByID(id string) (*healthcare.Message, error) {
	path := fmt.Sprintf("%s/messages/%s", genStoreName(c.config), id)
	return c.Get(path)
}

func (c *client) Get(path string) (*healthcare.Message, error) {
	c.limiter.Take()

	get := c.store.Messages.Get(path)
	get.Header().Add(apiFormatHeader, "2")

	ctx := context.Background()
	return get.Context(ctx).Do()
}

func initHL7Store(ctx context.Context, cred string) (*healthcare.ProjectsLocationsDatasetsHl7V2StoresService, error) {
	fmt.Println("initializing hcapi client")

	ts, err := tokenSource(ctx, cred, scope)
	if err != nil {
		return nil, err
	}

	hcs, err := healthcare.NewService(ctx, option.WithTokenSource(ts))
	if err != nil {
		return nil, err
	}

	return hcs.Projects.Locations.Datasets.Hl7V2Stores, nil
}

func tokenSource(ctx context.Context, cred string, scopes ...string) (oauth2.TokenSource, error) {
	fmt.Println("getting token source for hcapi client")

	if cred == "" {
		return goauth2.DefaultTokenSource(ctx, scopes...)
	}

	j, err := ioutil.ReadFile(cred)
	if err != nil {
		return nil, err
	}

	c, err := goauth2.CredentialsFromJSON(ctx, j, scopes...)
	if err != nil {
		return nil, err
	}

	return c.TokenSource, nil
}

func genStoreName(config Config) string {
	fmt.Println("generating store name on hcapi client")

	return strings.Join([]string{
		projectsPath,
		config.ProjectID,
		locationsPath,
		config.LocationID,
		datasetsPath,
		config.DatasetID,
		hl7storePath,
		config.HL7StoreID,
	}, "/")
}

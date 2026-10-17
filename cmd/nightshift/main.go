package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pranavkhaspa/knightly/internal/actions"
	"github.com/pranavkhaspa/knightly/internal/agents"
	"github.com/pranavkhaspa/knightly/internal/auth"
	"github.com/pranavkhaspa/knightly/internal/calendar"
	"github.com/pranavkhaspa/knightly/internal/chat"
	"github.com/pranavkhaspa/knightly/internal/config"
	"github.com/pranavkhaspa/knightly/internal/falkor"
	"github.com/pranavkhaspa/knightly/internal/github"
	"github.com/pranavkhaspa/knightly/internal/googleauth"
	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/ingestion"
	"github.com/pranavkhaspa/knightly/internal/mail"
	"github.com/pranavkhaspa/knightly/internal/mcp"
	"github.com/pranavkhaspa/knightly/internal/memory"
	"github.com/pranavkhaspa/knightly/internal/observability"
	"github.com/pranavkhaspa/knightly/internal/profile"
	"github.com/pranavkhaspa/knightly/internal/project"
	"github.com/pranavkhaspa/knightly/internal/recommendation"
	"github.com/pranavkhaspa/knightly/internal/reminder"
	"github.com/pranavkhaspa/knightly/internal/requirement"
	"github.com/pranavkhaspa/knightly/internal/scheduler"
	"github.com/pranavkhaspa/knightly/internal/search"
	"github.com/pranavkhaspa/knightly/internal/session"
	"github.com/pranavkhaspa/knightly/internal/untrusted"
	"github.com/pranavkhaspa/knightly/internal/web"
)

type initializationState string

const (
	stateStarting     initializationState = "starting"
	stateInitializing initializationState = "initializing"
	stateReady        initializationState = "ready"
	stateFailed       initializationState = "failed"
	stateStopping     initializationState = "stopping"
	stateStopped      initializationState = "stopped"
)

type application struct {
	config       config.Config
	scope        identity.Scope
	graph        *falkor.Client
	startedAt    time.Time
	pipeline     *ingestion.Pipeline
	ranking      *recommendation.Service
	memory       *memory.Service
	coordinator  *agents.Coordinator
	actions      *actions.Service
	chat         *chat.Service
	search       search.SearchProvider
	evidence     search.EvidenceStore
	mail         *mail.Service
	calendar     *calendar.Service
	reminders    *reminder.Service
	github       *github.Service
	githubExec   *github.Executor
	tools        *mcp.Server
	profiles     *profile.Service
	projects     *project.Service
	requirements *requirement.Service
	sessions     *session.Service
	sources      *falkor.SourceRepository
	adapters     *ingestion.AdapterRegistry
	collector    *ingestion.Collector
	schedule     *scheduler.Scheduler
	metrics      *observability.Metrics

	lifecycleMu       sync.Mutex
	stateMu           sync.RWMutex
	initialization    initializationState
	initializationErr string
	collection        collectionState
}

type collectionState struct {
	StartedAt       time.Time `json:"started_at,omitempty"`
	FinishedAt      time.Time `json:"finished_at,omitempty"`
	Sources         int       `json:"sources"`
	Succeeded       int       `json:"succeeded"`
	Failed          int       `json:"failed"`
	Skipped         int       `json:"skipped"`
	Items           int       `json:"items"`
	Recommendations int       `json:"recommendations"`
	Handoffs        int       `json:"handoffs"`
	Error           string    `json:"error,omitempty"`
}

type collectionRunner struct{ app *application }

func (r collectionRunner) Run(ctx context.Context) error {
	if r.app == nil {
		return errors.New("collection runner application is required")
	}
	return r.app.collect(ctx, false)
}

func (r collectionRunner) RunManual(ctx context.Context) error {
	if r.app == nil {
		return errors.New("collection runner application is required")
	}
	return r.app.collect(ctx, true)
}

// metricsRunner wraps the collection runner so scheduled runs contribute
// scheduler latency/outcome and briefing-run metrics. The scheduled run is the
// only daily pipeline today; the briefing counter moves with the dedicated
// briefing generator when that lands.
type metricsRunner struct {
	inner   collectionRunner
	metrics *observability.Metrics
}

func (r metricsRunner) Run(ctx context.Context) error {
	start := time.Now()
	err := r.inner.Run(ctx)
	r.metrics.ObserveScheduler(time.Since(start), err)
	r.metrics.RecordBriefing(runStatus(err))
	return err
}

func (r metricsRunner) RunManual(ctx context.Context) error {
	start := time.Now()
	err := r.inner.RunManual(ctx)
	r.metrics.ObserveScheduler(time.Since(start), err)
	r.metrics.RecordBriefing(runStatus(err))
	return err
}

func runStatus(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

// providerHTTPClient returns an HTTP client whose transport records provider
// latency and errors into metrics.
func providerHTTPClient(provider string, timeout time.Duration, metrics *observability.Metrics) *http.Client {
	return &http.Client{Timeout: timeout, Transport: observability.NewTransport(provider, nil, metrics)}
}

func newApplication(cfg config.Config, graph *falkor.Client, metrics *observability.Metrics) (*application, error) {
	scope := identity.Scope{TenantID: cfg.TenantID, UserID: cfg.UserID}
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("invalid installation scope: %w", err)
	}
	if graph == nil {
		return nil, errors.New("FalkorDB client is required")
	}
	selector, err := falkor.NewSingleUserGraphSelector(graph, scope)
	if err != nil {
		return nil, fmt.Errorf("construct trusted graph selector: %w", err)
	}

	memoryService := memory.NewService(falkor.NewMemoryRepository(graph))
	app := &application{
		config:         cfg,
		scope:          scope,
		graph:          graph,
		startedAt:      time.Now().UTC(),
		pipeline:       ingestion.NewPipeline(graph),
		ranking:        recommendation.NewService(graph),
		memory:         memoryService,
		coordinator:    agents.NewCoordinator(falkor.NewAgentRepository(graph)),
		actions:        actions.NewService(falkor.NewActionRepository(graph), cfg.ActionsEnabled),
		chat:           chat.NewService(falkor.NewChatRepository(selector)),
		profiles:       profile.NewService(falkor.NewProfileRepository(selector)),
		projects:       project.NewService(falkor.NewProjectRepository(selector)),
		requirements:   requirement.NewService(falkor.NewRequirementRepository(selector)),
		sessions:       session.NewService(falkor.NewSessionDecisionRepository(selector)),
		sources:        falkor.NewSourceRepository(graph, scope),
		metrics:        metrics,
		initialization: stateStarting,
	}
	if cfg.SearchURL != "" {
		provider, providerErr := search.NewSearXNGProvider(search.SearXNGConfig{
			BaseURL:           cfg.SearchURL,
			Client:            providerHTTPClient("searxng", 10*time.Second, metrics),
			AllowInsecureHTTP: strings.EqualFold(cfg.Environment, "development"),
		})
		if providerErr != nil {
			return nil, fmt.Errorf("construct search provider: %w", providerErr)
		}
		resilient, resilientErr := search.NewResilientProvider(provider, search.ResilienceConfig{})
		if resilientErr != nil {
			return nil, fmt.Errorf("construct resilient search provider: %w", resilientErr)
		}
		app.search = resilient
		app.evidence = falkor.NewSearchEvidenceRepository(selector)
	}
	credentialStore, credentialErr := mail.NewFileCredentialStore(cfg.MailCredentialsFile)
	if credentialErr != nil {
		return nil, fmt.Errorf("construct mail credential store: %w", credentialErr)
	}
	var oauthClient *mail.OAuthClient
	if cfg.GmailClientID != "" {
		oauthClient, credentialErr = mail.NewOAuthClient(mail.OAuthConfig{
			ClientID: cfg.GmailClientID, ClientSecret: cfg.GmailClientSecret, RedirectURL: cfg.GmailRedirectURL,
		})
		if credentialErr != nil {
			return nil, fmt.Errorf("construct mail OAuth client: %w", credentialErr)
		}
	}
	mailService, mailErr := mail.NewService(mail.ServiceDeps{
		Accounts:    falkor.NewMailAccountRepository(selector),
		Messages:    falkor.NewMailMessageRepository(selector),
		Credentials: credentialStore,
		OAuth:       oauthClient,
		NewReader: func(_ context.Context, scope identity.Scope, account mail.Account) (mail.MailReader, error) {
			return mail.NewGmailReader(mail.GmailConfig{
				Scope: scope, Account: account, OAuth: oauthClient, Credentials: credentialStore,
				Client: providerHTTPClient("google", 20*time.Second, metrics),
			})
		},
	})
	if mailErr != nil {
		return nil, fmt.Errorf("construct mail service: %w", mailErr)
	}
	app.mail = mailService
	var googleOAuth *googleauth.Client
	if cfg.GmailClientID != "" {
		googleOAuth, credentialErr = googleauth.NewClient(googleauth.Config{
			ClientID: cfg.GmailClientID, ClientSecret: cfg.GmailClientSecret, RedirectURL: cfg.GmailRedirectURL,
		})
		if credentialErr != nil {
			return nil, fmt.Errorf("construct calendar OAuth client: %w", credentialErr)
		}
	}
	location := calendar.LoadLocation(cfg.Timezone)
	calendarService, calendarErr := calendar.NewService(calendar.ServiceDeps{
		Accounts:    falkor.NewCalendarAccountRepository(selector),
		Events:      falkor.NewCalendarEventRepository(selector),
		Credentials: credentialStore,
		OAuth:       googleOAuth,
		Location:    location,
		NewReader: func(_ context.Context, scope identity.Scope, account calendar.Account) (calendar.CalendarReader, error) {
			return calendar.NewGoogleCalendarReader(calendar.GoogleConfig{
				Scope: scope, Account: account, Location: location, OAuth: googleOAuth, Credentials: credentialStore,
				Client: providerHTTPClient("google", 20*time.Second, metrics),
			})
		},
	})
	if calendarErr != nil {
		return nil, fmt.Errorf("construct calendar service: %w", calendarErr)
	}
	app.calendar = calendarService
	reminderStore := falkor.NewReminderRepository(selector)
	var reminderSinks []reminder.Sink
	if cfg.ReminderWebhookURL != "" {
		webhookSink, webhookErr := reminder.NewHTTPWebhookSink(cfg.ReminderWebhookURL, providerHTTPClient("webhook", 10*time.Second, metrics))
		if webhookErr != nil {
			return nil, fmt.Errorf("construct reminder webhook sink: %w", webhookErr)
		}
		reminderSinks = append(reminderSinks, webhookSink)
	}
	reminderService, reminderErr := reminder.NewService(reminder.ServiceDeps{
		Store: reminderStore, Inbox: reminderStore, Sinks: reminderSinks, Timezone: cfg.Timezone,
	})
	if reminderErr != nil {
		return nil, fmt.Errorf("construct reminder service: %w", reminderErr)
	}
	app.reminders = reminderService
	githubAllowlist, githubAllowlistErr := github.NewAllowlist(cfg.GitHubRepositories)
	if githubAllowlistErr != nil {
		return nil, fmt.Errorf("construct GitHub allowlist: %w", githubAllowlistErr)
	}
	if cfg.GitHubToken != "" {
		githubReader, githubReaderErr := github.NewClient(github.ClientConfig{
			Token: cfg.GitHubToken, BaseURL: cfg.GitHubAPIBase, Allowlist: githubAllowlist,
			Client: providerHTTPClient("github", 20*time.Second, metrics),
		})
		if githubReaderErr != nil {
			return nil, fmt.Errorf("construct GitHub client: %w", githubReaderErr)
		}
		githubService, githubServiceErr := github.NewService(githubReader, falkor.NewGitHubObjectRepository(selector))
		if githubServiceErr != nil {
			return nil, fmt.Errorf("construct GitHub service: %w", githubServiceErr)
		}
		app.github = githubService
		githubWriter, githubWriterErr := github.NewHTTPWriter(github.ClientConfig{
			Token: cfg.GitHubToken, BaseURL: cfg.GitHubAPIBase, Allowlist: githubAllowlist,
			Client: providerHTTPClient("github", 20*time.Second, metrics),
		})
		if githubWriterErr != nil {
			return nil, fmt.Errorf("construct GitHub writer: %w", githubWriterErr)
		}
		githubExecutor, githubExecutorErr := github.NewExecutor(githubWriter, githubAllowlist, app.scope)
		if githubExecutorErr != nil {
			return nil, fmt.Errorf("construct GitHub executor: %w", githubExecutorErr)
		}
		app.githubExec = githubExecutor
	}
	app.adapters = ingestion.NewHTTPAdapterRegistry(providerHTTPClient("ingestion", 15*time.Second, metrics), 2<<20, "NightShift/0.1")
	fixture := ingestion.DefaultFixtureSource()
	fixtureConfig := fixture.Config()
	if err := app.adapters.Register("fixture", func(source ingestion.SourceConfig) ingestion.SourceAdapter {
		// The fixture is deterministic and manual/demo-safe. Refuse to turn an
		// arbitrary persisted fixture record into a hidden data source.
		if source.ID != fixtureConfig.ID || source.URL != fixtureConfig.URL {
			return nil
		}
		return ingestion.DefaultFixtureSource()
	}); err != nil {
		return nil, fmt.Errorf("register fixture adapter: %w", err)
	}
	collector, err := ingestion.NewCollector(app.sources, app.pipeline, app.adapters)
	if err != nil {
		return nil, fmt.Errorf("construct collector: %w", err)
	}
	app.collector = collector
	app.collector.Coordinator = graph
	app.collector.LeaseOwner = cfg.InstanceID
	app.collector.LeaseLifetime = 10 * time.Minute
	app.schedule, err = scheduler.New(scheduler.Config{}, metricsRunner{inner: collectionRunner{app: app}, metrics: metrics})
	if err != nil {
		return nil, fmt.Errorf("construct scheduler: %w", err)
	}
	app.tools = mcp.NewServer(mcp.Dependencies{
		Memory:   memoryService,
		Context:  graph,
		Planner:  graph,
		Feedback: graph,
		Calls:    falkor.NewToolCallRepository(graph),
	})
	return app, nil
}

type statusResponse struct {
	Service             string              `json:"service"`
	Version             string              `json:"version"`
	Environment         string              `json:"environment"`
	InstanceID          string              `json:"instance_id"`
	Uptime              string              `json:"uptime"`
	GoVersion           string              `json:"go_version"`
	Architecture        string              `json:"architecture"`
	Graph               falkor.Status       `json:"graph"`
	Actions             bool                `json:"actions_enabled"`
	Initialization      initializationState `json:"initialization"`
	InitializationError string              `json:"initialization_error,omitempty"`
	Scope               identity.Scope      `json:"scope"`
	Scheduler           schedulerResponse   `json:"scheduler"`
	Collection          collectionState     `json:"last_collection"`
}

type schedulerResponse struct {
	Started           bool          `json:"started"`
	Paused            bool          `json:"paused"`
	Running           bool          `json:"running"`
	Pending           bool          `json:"pending"`
	StartedAt         time.Time     `json:"started_at,omitempty"`
	LastRunStartedAt  time.Time     `json:"last_run_started_at,omitempty"`
	LastRunFinishedAt time.Time     `json:"last_run_finished_at,omitempty"`
	LastRunDuration   time.Duration `json:"last_run_duration_ns,omitempty"`
	LastRunError      string        `json:"last_run_error,omitempty"`
	RunCount          uint64        `json:"run_count"`
	CoalescedCount    uint64        `json:"coalesced_count"`
	TimedOutCount     uint64        `json:"timed_out_count"`
}

func schedulerStatus(value scheduler.Status) schedulerResponse {
	return schedulerResponse{
		Started: value.Started, Paused: value.Paused, Running: value.Running, Pending: value.Pending,
		StartedAt: value.StartedAt, LastRunStartedAt: value.LastRunStartedAt,
		LastRunFinishedAt: value.LastRunFinishedAt, LastRunDuration: value.LastRunDuration,
		LastRunError: value.LastRunError, RunCount: value.RunCount,
		CoalescedCount: value.CoalescedCount, TimedOutCount: value.TimedOutCount,
	}
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	logger := observability.NewLogger(observability.LogConfig{
		Level: cfg.LogLevel, Format: cfg.LogFormat, Service: "nightshift", InstanceID: cfg.InstanceID,
	})
	slog.SetDefault(logger)
	metrics := observability.NewMetrics()

	graph, err := falkor.New(cfg.FalkorURL, cfg.ControlGraph, cfg.UserGraph, cfg.QueryTimeoutMillis)
	if err != nil {
		log.Fatalf("FalkorDB configuration error: %v", err)
	}
	defer graph.Close()
	graph.AddHook(observability.NewGraphHook(metrics))
	app, err := newApplication(cfg, graph, metrics)
	if err != nil {
		log.Fatalf("application construction error: %v", err)
	}

	exporter, err := observability.NewExporter(observability.OTLPConfig{
		Endpoint: cfg.OTLPEndpoint, Service: "nightshift", InstanceID: cfg.InstanceID,
	}, metrics.Registry(), metrics)
	if err != nil {
		log.Fatalf("OTLP exporter configuration error: %v", err)
	}
	exporter.Start()
	defer func() {
		stopContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if stopErr := exporter.Stop(stopContext); stopErr != nil {
			logger.Warn("event=otlp_exporter_stop_failed", "error", stopErr)
		}
	}()

	resolver, err := identityResolver(cfg)
	if err != nil {
		log.Fatalf("identity resolver configuration error: %v", err)
	}

	publicPaths := []string{"/health/live"}
	if cfg.MetricsPublic {
		publicPaths = append(publicPaths, "/metrics")
	}
	policy, err := auth.New(auth.Config{
		Token: cfg.AuthToken, Username: cfg.AuthUsername, Password: cfg.AuthPassword,
		Scope:      identity.Scope{TenantID: cfg.TenantID, UserID: cfg.UserID},
		ApproverID: cfg.AuthApproverID, RequireAuthentication: cfg.AuthRequired,
		PublicPaths: publicPaths,
		Resolver:    resolver,
	})
	if err != nil {
		log.Fatalf("authentication configuration error: %v", err)
	}
	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           observability.Middleware(logger, metrics, policy.Wrap(app.routes())),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Keep liveness independent from FalkorDB. Initialization and scheduler
	// startup happen in the background; readiness exposes their exact state.
	go func() {
		if err := app.initialize(context.Background()); err != nil {
			logger.Error("application initialization failed; HTTP health remains available", "error", err)
			return
		}
		logger.Info("application ready", "tenant_id", cfg.TenantID, "user_id", cfg.UserID)
	}()

	stopContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-stopContext.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			logger.Warn("event=shutdown_failed", "error", err)
		}
		if err := app.stop(shutdownContext); err != nil {
			logger.Warn("event=scheduler_shutdown_failed", "error", err)
		}
	}()

	logger.Info("server started", "listen", cfg.Listen, "instance_id", cfg.InstanceID)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func (a *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", a.live)
	mux.HandleFunc("GET /health/ready", a.ready)
	mux.HandleFunc("GET /metrics", a.metricsEndpoint)
	mux.HandleFunc("GET /api/v1/status", a.status)
	mux.HandleFunc("GET /api/v1/setup", a.setupRead)
	mux.HandleFunc("POST /api/v1/setup", a.setup)
	mux.HandleFunc("GET /api/v1/profile", a.profileRead)
	mux.HandleFunc("POST /api/v1/profile", a.profileSave)
	mux.HandleFunc("PUT /api/v1/profile", a.profileSave)
	mux.HandleFunc("GET /api/v1/projects", a.projectsRead)
	mux.HandleFunc("POST /api/v1/projects", a.projectSave)
	mux.HandleFunc("GET /api/v1/projects/{id}", a.projectRead)
	mux.HandleFunc("PUT /api/v1/projects/{id}", a.projectSave)
	mux.HandleFunc("GET /api/v1/projects/{project_id}/requirements", a.requirementsRead)
	mux.HandleFunc("POST /api/v1/projects/{project_id}/requirements", a.requirementSave)
	mux.HandleFunc("GET /api/v1/requirements", a.requirementsRead)
	mux.HandleFunc("POST /api/v1/requirements", a.requirementSave)
	mux.HandleFunc("GET /api/v1/requirements/{id}", a.requirementRead)
	mux.HandleFunc("PUT /api/v1/requirements/{id}", a.requirementSave)
	mux.HandleFunc("GET /api/v1/sessions/{id}", a.sessionRead)
	mux.HandleFunc("POST /api/v1/sessions", a.sessionSave)
	mux.HandleFunc("PUT /api/v1/sessions/{id}", a.sessionSave)
	mux.HandleFunc("GET /api/v1/conversations", a.conversationsRead)
	mux.HandleFunc("POST /api/v1/conversations", a.conversationCreate)
	mux.HandleFunc("GET /api/v1/conversations/{id}", a.conversationRead)
	mux.HandleFunc("PATCH /api/v1/conversations/{id}", a.conversationUpdate)
	mux.HandleFunc("GET /api/v1/conversations/{id}/messages", a.messagesRead)
	mux.HandleFunc("POST /api/v1/conversations/{id}/messages", a.messageCreate)
	mux.HandleFunc("GET /api/v1/conversations/{id}/events", a.eventsStream)
	mux.HandleFunc("GET /api/v1/search", a.searchRead)
	mux.HandleFunc("GET /api/v1/mail/accounts", a.mailAccountsRead)
	mux.HandleFunc("POST /api/v1/mail/accounts/connect", a.mailConnect)
	mux.HandleFunc("GET /api/v1/mail/accounts/{id}/status", a.mailAccountStatus)
	mux.HandleFunc("POST /api/v1/mail/accounts/{id}/disconnect", a.mailDisconnect)
	mux.HandleFunc("GET /api/v1/mail/messages", a.mailMessagesRead)
	mux.HandleFunc("GET /api/v1/mail/messages/{id}", a.mailMessageRead)
	mux.HandleFunc("GET /api/v1/calendar/accounts", a.calendarAccountsRead)
	mux.HandleFunc("GET /api/v1/calendar/accounts/authorize", a.calendarAuthorize)
	mux.HandleFunc("POST /api/v1/calendar/accounts/connect", a.calendarConnect)
	mux.HandleFunc("GET /api/v1/calendar/accounts/{id}/status", a.calendarAccountStatus)
	mux.HandleFunc("POST /api/v1/calendar/accounts/{id}/disconnect", a.calendarDisconnect)
	mux.HandleFunc("GET /api/v1/calendar/events", a.calendarEventsRead)
	mux.HandleFunc("GET /api/v1/calendar/events/stored", a.calendarStoredEventsRead)
	mux.HandleFunc("GET /api/v1/calendar/events/{id}", a.calendarEventRead)
	mux.HandleFunc("GET /api/v1/reminders", a.reminderList)
	mux.HandleFunc("POST /api/v1/reminders", a.reminderCreate)
	mux.HandleFunc("GET /api/v1/reminders/{id}", a.reminderRead)
	mux.HandleFunc("POST /api/v1/reminders/{id}/cancel", a.reminderCancel)
	mux.HandleFunc("POST /api/v1/reminders/{id}/approve", a.reminderApprove)
	mux.HandleFunc("POST /api/v1/reminders/tick", a.reminderTick)
	mux.HandleFunc("GET /api/v1/inbox", a.inboxRead)
	mux.HandleFunc("GET /api/v1/github/repositories/{owner}/{name}", a.githubRepositoryRead)
	mux.HandleFunc("GET /api/v1/github/issues", a.githubIssuesRead)
	mux.HandleFunc("GET /api/v1/github/pulls", a.githubPullRequestsRead)
	mux.HandleFunc("GET /api/v1/github/checks", a.githubChecksRead)
	mux.HandleFunc("GET /api/v1/github/releases", a.githubReleasesRead)
	mux.HandleFunc("GET /api/v1/github/objects", a.githubObjectsRead)
	mux.HandleFunc("GET /api/v1/decisions", a.decisionsRead)
	mux.HandleFunc("POST /api/v1/decisions", a.decisionSave)
	mux.HandleFunc("GET /api/v1/decisions/{id}", a.decisionRead)
	mux.HandleFunc("PUT /api/v1/decisions/{id}", a.decisionSave)
	mux.HandleFunc("GET /api/v1/sources", a.sourcesRead)
	mux.HandleFunc("POST /api/v1/sources", a.sourceSave)
	mux.HandleFunc("GET /api/v1/sources/{id}", a.sourceRead)
	mux.HandleFunc("PUT /api/v1/sources/{id}", a.sourceSave)
	mux.HandleFunc("DELETE /api/v1/sources/{id}", a.sourceDelete)
	mux.HandleFunc("GET /api/v1/dashboard", a.dashboard)
	mux.HandleFunc("POST /api/v1/runs", a.run)
	mux.HandleFunc("GET /api/v1/scheduler/status", a.schedulerStatus)
	mux.HandleFunc("POST /api/v1/scheduler/pause", a.schedulerPause)
	mux.HandleFunc("POST /api/v1/scheduler/resume", a.schedulerResume)
	mux.HandleFunc("GET /api/v1/recommendations", a.recommendations)
	mux.HandleFunc("GET /api/v1/graph/path/{id}", a.graphPath)
	mux.HandleFunc("POST /api/v1/recommendations/{id}/feedback", a.feedback)
	mux.HandleFunc("POST /api/v1/recommendations/{id}/plan", a.plan)
	mux.HandleFunc("GET /api/v1/memories", a.memories)
	mux.HandleFunc("POST /api/v1/actions/{id}/approve", a.approveAction)
	mux.HandleFunc("POST /api/v1/actions/{id}/execute", a.executeAction)
	mux.HandleFunc("GET /api/v1/mcp/tools", a.mcpTools)
	mux.HandleFunc("POST /api/v1/mcp", a.mcpInvoke)
	mux.Handle("GET /", web.Handler())
	return requestID(mux)
}

func (a *application) initialize(ctx context.Context) error {
	a.lifecycleMu.Lock()
	state, _, _ := a.initializationStatus()
	if state == stateStopping || state == stateStopped {
		a.lifecycleMu.Unlock()
		return context.Canceled
	}
	a.setInitialization(stateInitializing, "")
	a.lifecycleMu.Unlock()
	if err := a.graph.Initialize(); err != nil {
		a.failInitialization(err.Error())
		return fmt.Errorf("initialize graph: %w", err)
	}
	if err := a.ensureSetup(ctx); err != nil {
		a.failInitialization(err.Error())
		return fmt.Errorf("complete setup: %w", err)
	}
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	state, _, _ = a.initializationStatus()
	if state == stateStopping || state == stateStopped || ctx.Err() != nil {
		return context.Canceled
	}
	if err := a.schedule.Start(); err != nil {
		a.setInitialization(stateFailed, err.Error())
		return fmt.Errorf("start scheduler: %w", err)
	}
	a.deliverReminders(ctx)
	a.setInitialization(stateReady, "")
	return nil
}

func (a *application) ensureSetup(ctx context.Context) error {
	if a.scope.TenantID != "tenant_demo" || a.scope.UserID != "user_demo" {
		if _, err := a.graph.EnsureUserGraph(ctx, a.scope); err != nil {
			return err
		}
	}
	if _, err := a.profiles.Get(ctx, a.scope); err != nil {
		if !errors.Is(err, profile.ErrProfileNotFound) {
			return err
		}
		if _, saveErr := a.profiles.Save(ctx, profile.Profile{
			TenantID:    a.scope.TenantID,
			UserID:      a.scope.UserID,
			DisplayName: "NightShift user",
		}); saveErr != nil {
			return saveErr
		}
	}
	if _, err := a.projects.Get(ctx, a.scope, a.config.ProjectID); err != nil {
		if !errors.Is(err, project.ErrProjectNotFound) {
			return err
		}
		if _, saveErr := a.projects.Save(ctx, project.Project{
			ID: a.config.ProjectID, TenantID: a.scope.TenantID, UserID: a.scope.UserID,
			Name: "NightShift", Description: "Graph-backed developer context", Status: project.Active,
			Technologies: []string{"FalkorDB"}, DesiredOutcomes: []string{"Explainable graph-backed recommendations"},
		}); saveErr != nil {
			return saveErr
		}
		if _, saveErr := a.requirements.Save(ctx, requirement.Requirement{
			ID: "requirement_graph_context", TenantID: a.scope.TenantID, UserID: a.scope.UserID,
			ProjectID: a.config.ProjectID, Title: "Use connected context for explainable recommendations",
			Status: requirement.Open, Priority: 1, TechnologyIDs: []string{"technology_falkordb"},
		}); saveErr != nil {
			return saveErr
		}
	}
	// The deterministic fixture is safe to register because its adapter only
	// resolves the fixed example.invalid record. It keeps the existing demo
	// workflow available to both manual and scheduled collection.
	if err := a.sources.UpsertSource(ctx, ingestion.DefaultFixtureSource().Config()); err != nil {
		return fmt.Errorf("register demo fixture source: %w", err)
	}
	return nil
}

func (a *application) stop(ctx context.Context) error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	a.setInitialization(stateStopping, "")
	if a.schedule != nil {
		if err := a.schedule.Stop(ctx); err != nil && !errors.Is(err, scheduler.ErrNotStarted) {
			a.setInitialization(stateFailed, err.Error())
			return err
		}
	}
	a.setInitialization(stateStopped, "")
	return nil
}

func (a *application) failInitialization(message string) {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	state, _, _ := a.initializationStatus()
	if state != stateStopping && state != stateStopped {
		a.setInitialization(stateFailed, message)
	}
}

func (a *application) setInitialization(state initializationState, message string) {
	a.stateMu.Lock()
	a.initialization = state
	a.initializationErr = message
	a.stateMu.Unlock()
}

func (a *application) initializationStatus() (initializationState, string, collectionState) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	return a.initialization, a.initializationErr, a.collection
}

func (a *application) recordCollection(summary ingestion.CollectionSummary, recommendationCount, handoffCount int, runErr error) {
	state := collectionState{
		StartedAt: summary.StartedAt, FinishedAt: summary.FinishedAt,
		Sources: summary.Sources, Succeeded: summary.Succeeded, Failed: summary.Failed,
		Skipped: summary.Skipped, Items: len(summary.Items),
		Recommendations: recommendationCount, Handoffs: handoffCount,
	}
	if runErr != nil {
		state.Error = runErr.Error()
	}
	a.stateMu.Lock()
	a.collection = state
	a.stateMu.Unlock()
}

func (a *application) collect(ctx context.Context, force bool) error {
	if a.collector == nil {
		return errors.New("collector is not configured")
	}
	a.deliverReminders(ctx)
	var (
		summary ingestion.CollectionSummary
		err     error
	)
	if force {
		summary, err = a.collector.RunNow(ctx)
	} else {
		summary, err = a.collector.Run(ctx)
	}
	if err != nil {
		a.recordCollection(summary, 0, 0, err)
		return err
	}
	recommendations, handoffs, recommendationErr := a.recommendCollected(ctx, summary)
	if recommendationErr != nil {
		a.recordCollection(summary, recommendations, handoffs, recommendationErr)
		return recommendationErr
	}
	if summary.Failed > 0 {
		err = fmt.Errorf("collection failed for %d source(s)", summary.Failed)
	}
	a.recordCollection(summary, recommendations, handoffs, err)
	return err
}

// deliverReminders reconciles and delivers due reminders. It never fails the
// surrounding collection run: delivery is retried on the next tick and recorded
// through the graph lease.
func (a *application) deliverReminders(ctx context.Context) {
	if a.reminders == nil {
		return
	}
	result, err := a.reminders.Tick(ctx, a.scope)
	if err != nil {
		slog.ErrorContext(ctx, "reminder tick failed", "error", err)
		a.metrics.RecordReminder("error", 1)
		return
	}
	a.metrics.RecordReminder("claimed", result.Claimed)
	a.metrics.RecordReminder("delivered", result.Delivered)
	a.metrics.RecordReminder("deferred", result.Deferred)
	a.metrics.RecordReminder("pending_approval", result.PendingApproval)
	if result.Claimed > 0 {
		slog.InfoContext(ctx, "reminder tick completed", "claimed", result.Claimed, "delivered", result.Delivered,
			"deferred", result.Deferred, "pending_approval", result.PendingApproval)
	}
}

func (a *application) recommendCollected(ctx context.Context, summary ingestion.CollectionSummary) (int, int, error) {
	if len(summary.Items) == 0 {
		return 0, 0, nil
	}
	projectID, err := a.collectionProject(ctx)
	if err != nil {
		return 0, 0, err
	}
	candidates := make([]recommendation.Candidate, 0, len(summary.Items))
	for _, item := range summary.Items {
		candidates = append(candidates, recommendation.Candidate{
			ItemID: item.ID, Title: item.Title, Summary: item.Summary,
			URL: item.CanonicalURL, SourceID: item.SourceID,
			ContentHash: item.ContentHash, PublishedAt: item.PublishedAt, Score: 1,
		})
	}
	recommendations, err := a.ranking.Recommend(ctx, a.scope.UserID, projectID, candidates)
	if err != nil {
		return 0, 0, fmt.Errorf("recommend collected items: %w", err)
	}
	handoffs := 0
	for _, value := range recommendations {
		now := time.Now().UTC()
		taskID := "task_relevance_" + value.ID
		if _, err := a.coordinator.CreateTask(ctx, agents.Task{
			ID: taskID, TenantID: a.scope.TenantID, UserID: a.scope.UserID,
			ProjectID: projectID, Agent: agents.RelevanceAgent, Status: agents.Running,
			Priority: 1, CorrelationID: "collection_" + summary.StartedAt.Format("20060102T150405.000000000Z"),
			InputRefs: []string{value.ID}, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			return len(recommendations), handoffs, fmt.Errorf("create relevance task: %w", err)
		}
		a.metrics.RecordTask("relevance", "created")
		if _, err := a.coordinator.Handoff(ctx, agents.HandoffRequest{
			ID: "handoff_" + value.ID, UserID: a.scope.UserID,
			FromTaskID: taskID, ToTaskID: "task_planner_" + value.ID,
			ToAgent:       agents.PlannerAgent,
			CorrelationID: "collection_" + summary.StartedAt.Format("20060102T150405.000000000Z"),
			Notes:         "Graph-backed collection recommendation is ready for a bounded plan.",
			InputRefs:     []string{value.ID},
		}); err != nil {
			return len(recommendations), handoffs, fmt.Errorf("handoff recommendation: %w", err)
		}
		a.metrics.RecordTask("handoff", "created")
		handoffs++
	}
	return len(recommendations), handoffs, nil
}

func (a *application) collectionProject(ctx context.Context) (string, error) {
	if a.config.ProjectID != "" {
		if _, err := a.projects.Get(ctx, a.scope, a.config.ProjectID); err == nil {
			return a.config.ProjectID, nil
		} else if !errors.Is(err, project.ErrProjectNotFound) {
			return "", err
		}
	}
	values, err := a.projects.List(ctx, a.scope, project.DefaultListLimit)
	if err != nil {
		return "", err
	}
	for _, value := range values {
		if value.Status == project.Active {
			return value.ID, nil
		}
	}
	if len(values) > 0 {
		return values[0].ID, nil
	}
	return "", errors.New("no project is configured for collection recommendations")
}

func (a *application) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *application) ready(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	initialization, initializationErr, _ := a.initializationStatus()
	status := a.graph.Status(ctx)
	if initialization != stateReady || !status.Available {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status":               "not_ready",
			"initialization":       initialization,
			"initialization_error": initializationErr,
			"graph":                status,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":         "ready",
		"initialization": initialization,
		"graph":          status,
	})
}

// metricsEndpoint renders the bounded Prometheus text exposition. It exposes
// counters and histograms only; no credential, prompt, or payload value is ever
// recorded in a metric label.
func (a *application) metricsEndpoint(w http.ResponseWriter, _ *http.Request) {
	registry := a.metrics.Registry()
	if registry == nil {
		writeError(w, http.StatusServiceUnavailable, "METRICS_UNAVAILABLE", errors.New("metrics registry is not configured"), false)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := registry.Render(w); err != nil {
		slog.Error("event=metrics_render_failed", "error", err)
	}
}

func (a *application) status(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	initialization, initializationErr, collection := a.initializationStatus()
	writeJSON(w, http.StatusOK, statusResponse{
		Service:             "nightshift",
		Version:             "0.1-foundation",
		Environment:         a.config.Environment,
		InstanceID:          a.config.InstanceID,
		Uptime:              time.Since(a.startedAt).Round(time.Second).String(),
		GoVersion:           runtime.Version(),
		Architecture:        runtime.GOOS + "/" + runtime.GOARCH,
		Graph:               a.graph.Status(ctx),
		Actions:             a.config.ActionsEnabled,
		Initialization:      initialization,
		InitializationError: initializationErr,
		Scope:               scope,
		Scheduler:           schedulerStatus(a.schedule.Status()),
		Collection:          collection,
	})
}

func (a *application) dashboard(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	dashboard, err := a.graph.Dashboard(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":  "dashboard_unavailable",
			"detail": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, dashboard)
}

func (a *application) requireReady(w http.ResponseWriter) bool {
	state, initializationErr, _ := a.initializationStatus()
	if state == stateReady {
		return true
	}
	detail := fmt.Sprintf("application is %s", state)
	if initializationErr != "" {
		detail += ": " + initializationErr
	}
	writeError(w, http.StatusServiceUnavailable, "APPLICATION_NOT_READY", errors.New(detail), true)
	return false
}

// resolvedScope returns the verified per-request scope installed by the auth
// middleware. It never reads tenant or user values from the request. The
// installation scope is only a fallback for loopback-only single-user
// deployments without a resolver and for direct handler unit tests.
func (a *application) resolvedScope(request *http.Request) identity.Scope {
	if scope, err := auth.ScopeFromRequest(request); err == nil {
		return scope
	}
	return a.scope
}

// resolvedApprover returns the verified approver identity. It never trusts a
// client-supplied approved_by field.
func (a *application) resolvedApprover(request *http.Request) string {
	if approver, err := auth.ApproverFromRequest(request); err == nil {
		return approver
	}
	return a.config.AuthApproverID
}

type setupResponse struct {
	State     initializationState `json:"state"`
	Scope     identity.Scope      `json:"scope"`
	Scheduler schedulerResponse   `json:"scheduler"`
}

func (a *application) setupRead(w http.ResponseWriter, _ *http.Request) {
	state, _, _ := a.initializationStatus()
	writeJSON(w, http.StatusOK, setupResponse{State: state, Scope: a.scope, Scheduler: schedulerStatus(a.schedule.Status())})
}

func (a *application) setup(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	if err := a.ensureSetup(ctx); err != nil {
		writeError(w, http.StatusBadGateway, "SETUP_FAILED", err, true)
		return
	}
	writeJSON(w, http.StatusOK, setupResponse{State: stateReady, Scope: scope, Scheduler: schedulerStatus(a.schedule.Status())})
}

func (a *application) profileRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.profiles.Get(request.Context(), scope)
	if err != nil {
		writeDomainError(w, "PROFILE_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) profileSave(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var value profile.Profile
	if err := decodeJSON(w, request, &value); err != nil {
		return
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	saved, err := a.profiles.Save(request.Context(), value)
	if err != nil {
		writeDomainError(w, "PROFILE_SAVE_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

func (a *application) projectsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	values, err := a.projects.List(request.Context(), scope, parseLimit(request.URL.Query().Get("limit"), project.DefaultListLimit))
	if err != nil {
		writeDomainError(w, "PROJECT_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"projects": values})
}

func (a *application) projectRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.projects.Get(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "PROJECT_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) projectSave(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var value project.Project
	if err := decodeJSON(w, request, &value); err != nil {
		return
	}
	if pathID := request.PathValue("id"); pathID != "" {
		value.ID = pathID
	}
	if value.ID == "" {
		value.ID = stableAPIID("project", scope.UserID+"\x00"+value.Name)
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	saved, err := a.projects.Save(request.Context(), value)
	if err != nil {
		writeDomainError(w, "PROJECT_SAVE_FAILED", err)
		return
	}
	status := http.StatusCreated
	if request.Method == http.MethodPut {
		status = http.StatusOK
	}
	writeJSON(w, status, saved)
}

func (a *application) requirementsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	projectID := request.PathValue("project_id")
	if projectID == "" {
		projectID = request.URL.Query().Get("project_id")
	}
	values, err := a.requirements.List(request.Context(), scope, projectID, parseLimit(request.URL.Query().Get("limit"), requirement.DefaultListLimit))
	if err != nil {
		writeDomainError(w, "REQUIREMENT_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"requirements": values})
}

func (a *application) requirementRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.requirements.Get(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "REQUIREMENT_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) requirementSave(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var value requirement.Requirement
	if err := decodeJSON(w, request, &value); err != nil {
		return
	}
	if pathID := request.PathValue("id"); pathID != "" {
		value.ID = pathID
	}
	if pathProjectID := request.PathValue("project_id"); pathProjectID != "" {
		value.ProjectID = pathProjectID
	}
	if value.ID == "" {
		value.ID = stableAPIID("requirement", scope.UserID+"\x00"+value.ProjectID+"\x00"+value.Title)
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	saved, err := a.requirements.Save(request.Context(), value)
	if err != nil {
		writeDomainError(w, "REQUIREMENT_SAVE_FAILED", err)
		return
	}
	status := http.StatusCreated
	if request.Method == http.MethodPut {
		status = http.StatusOK
	}
	writeJSON(w, status, saved)
}

func (a *application) sessionRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.sessions.GetSession(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "SESSION_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) sessionSave(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var value session.Session
	if err := decodeJSON(w, request, &value); err != nil {
		return
	}
	if pathID := request.PathValue("id"); pathID != "" {
		value.ID = pathID
	}
	if value.ID == "" {
		value.ID = stableAPIID("session", scope.UserID+"\x00"+value.Title)
	}
	if value.ProjectID == "" {
		value.ProjectID = a.config.ProjectID
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	saved, err := a.sessions.SaveSession(request.Context(), value)
	if err != nil {
		writeDomainError(w, "SESSION_SAVE_FAILED", err)
		return
	}
	status := http.StatusCreated
	if request.Method == http.MethodPut {
		status = http.StatusOK
	}
	writeJSON(w, status, saved)
}

func (a *application) decisionsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	values, err := a.sessions.ListDecisions(request.Context(), scope, request.URL.Query().Get("session_id"), parseLimit(request.URL.Query().Get("limit"), session.DefaultListLimit))
	if err != nil {
		writeDomainError(w, "DECISION_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"decisions": values})
}

func (a *application) decisionRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.sessions.GetDecision(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "DECISION_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) decisionSave(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var value session.Decision
	if err := decodeJSON(w, request, &value); err != nil {
		return
	}
	if pathID := request.PathValue("id"); pathID != "" {
		value.ID = pathID
	}
	if value.ID == "" {
		value.ID = stableAPIID("decision", scope.UserID+"\x00"+value.SessionID+"\x00"+value.Subject)
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	saved, err := a.sessions.SaveDecision(request.Context(), value)
	if err != nil {
		writeDomainError(w, "DECISION_SAVE_FAILED", err)
		return
	}
	status := http.StatusCreated
	if request.Method == http.MethodPut {
		status = http.StatusOK
	}
	writeJSON(w, status, saved)
}

type conversationRequest struct {
	Title     string `json:"title,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

type conversationUpdateRequest struct {
	Title  *string                  `json:"title,omitempty"`
	Status *chat.ConversationStatus `json:"status,omitempty"`
}

type messageRequest struct {
	ID      string `json:"id,omitempty"`
	TurnID  string `json:"turn_id,omitempty"`
	Content string `json:"content"`
}

func (a *application) conversationsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	values, err := a.chat.ListConversations(request.Context(), scope, parseLimit(request.URL.Query().Get("limit"), chat.DefaultLimit))
	if err != nil {
		writeDomainError(w, "CONVERSATION_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"conversations": values})
}

func (a *application) conversationCreate(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var body conversationRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	value, err := a.chat.CreateConversation(request.Context(), scope, body.ProjectID, body.Title)
	if err != nil {
		writeDomainError(w, "CONVERSATION_CREATE_FAILED", err)
		return
	}
	_, _ = a.chat.AppendEvent(request.Context(), scope, chat.Event{
		ID:             stableAPIID("event", value.ID+"\x00created"),
		ConversationID: value.ID,
		Type:           chat.ConversationCreated,
		Data:           map[string]string{"title": value.Title},
	})
	writeJSON(w, http.StatusCreated, value)
}

func (a *application) conversationRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.chat.GetConversation(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "CONVERSATION_READ_FAILED", err)
		return
	}
	messages, err := a.chat.ListMessages(request.Context(), scope, value.ID, parseLimit(request.URL.Query().Get("message_limit"), chat.DefaultLimit))
	if err != nil {
		writeDomainError(w, "MESSAGE_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"conversation": value, "messages": messages})
}

func (a *application) conversationUpdate(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var body conversationUpdateRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	value, err := a.chat.GetConversation(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "CONVERSATION_READ_FAILED", err)
		return
	}
	if body.Title != nil {
		value.Title = *body.Title
	}
	if body.Status != nil {
		value.Status = *body.Status
	}
	updated, err := a.chat.UpdateConversation(request.Context(), scope, value)
	if err != nil {
		writeDomainError(w, "CONVERSATION_UPDATE_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (a *application) messagesRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	conversationID := request.PathValue("id")
	if _, err := a.chat.GetConversation(request.Context(), scope, conversationID); err != nil {
		writeDomainError(w, "CONVERSATION_READ_FAILED", err)
		return
	}
	values, err := a.chat.ListMessages(request.Context(), scope, conversationID, parseLimit(request.URL.Query().Get("limit"), chat.DefaultLimit))
	if err != nil {
		writeDomainError(w, "MESSAGE_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"messages": values})
}

func (a *application) messageCreate(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	conversationID := request.PathValue("id")
	if _, err := a.chat.GetConversation(request.Context(), scope, conversationID); err != nil {
		writeDomainError(w, "CONVERSATION_READ_FAILED", err)
		return
	}
	var body messageRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	requestID := requestIdentifier(request)
	turnID := body.TurnID
	if turnID == "" {
		turnID = stableAPIID("turn", conversationID+"\x00"+requestID)
	}
	messageID := body.ID
	if messageID == "" {
		messageID = stableAPIID("message", conversationID+"\x00"+requestID)
	}
	userMessage, err := a.chat.AddMessage(request.Context(), scope, chat.Message{
		ID: messageID, ConversationID: conversationID, TurnID: turnID,
		Role: chat.RoleUser, Content: body.Content,
	})
	if err != nil {
		writeDomainError(w, "MESSAGE_CREATE_FAILED", err)
		return
	}
	if _, err := a.chat.AppendEvent(request.Context(), scope, chat.Event{
		ID: stableAPIID("event", turnID+"\x00message"), ConversationID: conversationID, TurnID: turnID,
		Type: chat.MessageCreated, Data: map[string]string{"message_id": userMessage.ID, "role": string(userMessage.Role)},
	}); err != nil {
		writeDomainError(w, "EVENT_APPEND_FAILED", err)
		return
	}
	if _, err := a.chat.AppendEvent(request.Context(), scope, chat.Event{
		ID: stableAPIID("event", turnID+"\x00started"), ConversationID: conversationID, TurnID: turnID,
		Type: chat.TurnStarted, Data: map[string]string{"message_id": userMessage.ID},
	}); err != nil {
		writeDomainError(w, "EVENT_APPEND_FAILED", err)
		return
	}

	assistantContent := "I recorded your request. Provider-backed search and coding actions will stream into this conversation as they are configured."
	var assistantCitations []chat.Citation
	if query, shouldSearch := chatSearchQuery(body.Content); shouldSearch && a.search != nil {
		_, _ = a.chat.AppendEvent(request.Context(), scope, chat.Event{
			ID: stableAPIID("event", turnID+"\x00search-started"), ConversationID: conversationID, TurnID: turnID,
			Type: chat.TurnProgress, Data: map[string]string{"phase": "search", "query": query},
		})
		result, searchErr := a.search.Search(request.Context(), search.SearchRequest{Query: query, Limit: 5})
		if searchErr != nil {
			assistantContent = "I could not complete the web search: " + searchErr.Error()
		} else {
			assistantContent, assistantCitations = formatSearchAnswer(result)
			if a.evidence != nil {
				if recorded, evidenceErr := a.evidence.SaveSearchEvidence(request.Context(), scope, query, result.Results); evidenceErr != nil {
					slog.ErrorContext(request.Context(), "search evidence save failed", "error", evidenceErr)
				} else {
					_, _ = a.chat.AppendEvent(request.Context(), scope, chat.Event{
						ID: stableAPIID("event", turnID+"\x00search-evidence"), ConversationID: conversationID, TurnID: turnID,
						Type: chat.TurnProgress, Data: map[string]string{"phase": "search", "evidence_count": strconv.Itoa(len(recorded))},
					})
				}
			}
		}
		_, _ = a.chat.AppendEvent(request.Context(), scope, chat.Event{
			ID: stableAPIID("event", turnID+"\x00search-completed"), ConversationID: conversationID, TurnID: turnID,
			Type: chat.TurnProgress, Data: map[string]string{"phase": "search", "status": "completed"},
		})
	}
	// This deterministic response keeps the durable chat flow useful before an
	// optional model or external search provider is configured. Provider-backed
	// responders preserve the same message and event records.
	assistantMessage, err := a.chat.AddMessage(request.Context(), scope, chat.Message{
		ID: stableAPIID("message", turnID+"\x00assistant"), ConversationID: conversationID, TurnID: turnID,
		Role: chat.RoleAssistant, Content: assistantContent, Citations: assistantCitations,
	})
	if err != nil {
		writeDomainError(w, "MESSAGE_CREATE_FAILED", err)
		return
	}
	if _, err := a.chat.AppendEvent(request.Context(), scope, chat.Event{
		ID: stableAPIID("event", turnID+"\x00assistant"), ConversationID: conversationID, TurnID: turnID,
		Type: chat.MessageCreated, Data: map[string]string{"message_id": assistantMessage.ID, "role": string(assistantMessage.Role)},
	}); err != nil {
		writeDomainError(w, "EVENT_APPEND_FAILED", err)
		return
	}
	if _, err := a.chat.AppendEvent(request.Context(), scope, chat.Event{
		ID: stableAPIID("event", turnID+"\x00completed"), ConversationID: conversationID, TurnID: turnID,
		Type: chat.AssistantCompleted, Data: map[string]string{"message_id": assistantMessage.ID},
	}); err != nil {
		writeDomainError(w, "EVENT_APPEND_FAILED", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"conversation_id": conversationID, "turn_id": turnID,
		"user_message": userMessage, "assistant_message": assistantMessage,
		"stream_url": "/api/v1/conversations/" + conversationID + "/events",
	})
}

func (a *application) eventsStream(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	after := int64(0)
	if value := request.Header.Get("Last-Event-ID"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_EVENT_CURSOR", errors.New("Last-Event-ID must be a non-negative integer"), false)
			return
		}
		after = parsed
	}
	if value := request.URL.Query().Get("after"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_EVENT_CURSOR", errors.New("after must be a non-negative integer"), false)
			return
		}
		after = parsed
	}
	conversationID := request.PathValue("id")
	if _, err := a.chat.GetConversation(request.Context(), scope, conversationID); err != nil {
		writeDomainError(w, "CONVERSATION_READ_FAILED", err)
		return
	}
	events, err := a.chat.ListEvents(request.Context(), scope, conversationID, after, parseLimit(request.URL.Query().Get("limit"), chat.MaxLimit))
	if err != nil {
		writeDomainError(w, "EVENT_LIST_FAILED", err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "SSE_UNSUPPORTED", errors.New("response writer does not support streaming"), true)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("Connection", "keep-alive")
	for _, value := range events {
		encoded, encodeErr := json.Marshal(value)
		if encodeErr != nil {
			continue
		}
		_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", value.Sequence, value.Type, encoded)
	}
	_, _ = fmt.Fprint(w, ": heartbeat\n\n")
	flusher.Flush()
}

func (a *application) searchRead(w http.ResponseWriter, request *http.Request) {
	if !a.requireReady(w) {
		return
	}
	if a.search == nil {
		writeError(w, http.StatusServiceUnavailable, "SEARCH_UNAVAILABLE", errors.New("NIGHTSHIFT_SEARCH_URL is not configured"), true)
		return
	}
	query := strings.TrimSpace(request.URL.Query().Get("q"))
	page := 1
	if value := request.URL.Query().Get("page"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_SEARCH_PAGE", err, false)
			return
		}
		page = parsed
	}
	limit := 0
	if value := request.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_SEARCH_LIMIT", err, false)
			return
		}
		limit = parsed
	}
	result, err := a.search.Search(request.Context(), search.SearchRequest{Query: query, Page: page, Limit: limit})
	if err != nil {
		writeDomainError(w, "SEARCH_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type mailConnectRequest struct {
	State        string    `json:"state,omitempty"`
	Code         string    `json:"code,omitempty"`
	EmailAddress string    `json:"email_address,omitempty"`
	AccessToken  string    `json:"access_token,omitempty"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	Expiry       time.Time `json:"expiry,omitempty"`
}

func (a *application) mailAccountsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	values, err := a.mail.Accounts(request.Context(), scope, parseLimit(request.URL.Query().Get("limit"), 20))
	if err != nil {
		writeDomainError(w, "MAIL_ACCOUNT_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"accounts": values})
}

func (a *application) mailConnect(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var body mailConnectRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	if strings.TrimSpace(body.Code) != "" && strings.TrimSpace(body.State) == "" {
		writeError(w, http.StatusBadRequest, "MAIL_STATE_REQUIRED", errors.New("state is required for the authorization-code flow"), false)
		return
	}
	account, err := a.mail.Connect(request.Context(), scope, mail.ConnectRequest{
		Code: body.Code, EmailAddress: body.EmailAddress,
		AccessToken: body.AccessToken, RefreshToken: body.RefreshToken, Expiry: body.Expiry,
	})
	if err != nil {
		writeDomainError(w, "MAIL_CONNECT_FAILED", err)
		return
	}
	writeJSON(w, http.StatusCreated, account)
}

func (a *application) mailAccountStatus(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	account, err := a.mail.Status(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "MAIL_ACCOUNT_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, account)
}

func (a *application) mailDisconnect(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	if err := a.mail.Disconnect(request.Context(), scope, request.PathValue("id")); err != nil {
		writeDomainError(w, "MAIL_DISCONNECT_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": string(mail.AccountDisconnected)})
}

func (a *application) mailMessagesRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	accountID := strings.TrimSpace(request.URL.Query().Get("account_id"))
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "MAIL_ACCOUNT_REQUIRED", errors.New("account_id is required"), false)
		return
	}
	limit := 0
	if value := request.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_MAIL_LIMIT", err, false)
			return
		}
		limit = parsed
	}
	page, err := a.mail.ListMessages(request.Context(), scope, accountID, mail.ListQuery{
		Query: request.URL.Query().Get("q"), PageToken: request.URL.Query().Get("page_token"), Limit: limit,
	})
	if err != nil {
		writeDomainError(w, "MAIL_MESSAGES_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"messages": page.Messages, "next_page_token": page.NextPageToken})
}

func (a *application) mailMessageRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	accountID := strings.TrimSpace(request.URL.Query().Get("account_id"))
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "MAIL_ACCOUNT_REQUIRED", errors.New("account_id is required"), false)
		return
	}
	message, err := a.mail.GetMessage(request.Context(), scope, accountID, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "MAIL_MESSAGE_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, message)
}

type calendarConnectRequest struct {
	State        string    `json:"state,omitempty"`
	Code         string    `json:"code,omitempty"`
	CalendarID   string    `json:"calendar_id,omitempty"`
	EmailAddress string    `json:"email_address,omitempty"`
	AccessToken  string    `json:"access_token,omitempty"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	Expiry       time.Time `json:"expiry,omitempty"`
}

func (a *application) calendarAccountsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	values, err := a.calendar.Accounts(request.Context(), scope, parseLimit(request.URL.Query().Get("limit"), 20))
	if err != nil {
		writeDomainError(w, "CALENDAR_ACCOUNT_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"accounts": values})
}

func (a *application) calendarAuthorize(w http.ResponseWriter, request *http.Request) {
	if !a.requireReady(w) {
		return
	}
	state := strings.TrimSpace(request.URL.Query().Get("state"))
	if state == "" {
		writeError(w, http.StatusBadRequest, "CALENDAR_STATE_REQUIRED", errors.New("state is required"), false)
		return
	}
	authorizationURL, err := a.calendar.AuthCodeURL(state)
	if err != nil {
		writeDomainError(w, "CALENDAR_AUTHORIZE_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"authorization_url": authorizationURL})
}

func (a *application) calendarConnect(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var body calendarConnectRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	if strings.TrimSpace(body.Code) != "" && strings.TrimSpace(body.State) == "" {
		writeError(w, http.StatusBadRequest, "CALENDAR_STATE_REQUIRED", errors.New("state is required for the authorization-code flow"), false)
		return
	}
	account, err := a.calendar.Connect(request.Context(), scope, calendar.ConnectRequest{
		Code: body.Code, CalendarID: body.CalendarID, EmailAddress: body.EmailAddress,
		AccessToken: body.AccessToken, RefreshToken: body.RefreshToken, Expiry: body.Expiry,
	})
	if err != nil {
		writeDomainError(w, "CALENDAR_CONNECT_FAILED", err)
		return
	}
	writeJSON(w, http.StatusCreated, account)
}

func (a *application) calendarAccountStatus(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	account, err := a.calendar.Status(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "CALENDAR_ACCOUNT_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, account)
}

func (a *application) calendarDisconnect(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	if err := a.calendar.Disconnect(request.Context(), scope, request.PathValue("id")); err != nil {
		writeDomainError(w, "CALENDAR_DISCONNECT_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": string(calendar.AccountDisconnected)})
}

func (a *application) calendarEventsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	query := request.URL.Query()
	accountID := strings.TrimSpace(query.Get("account_id"))
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "CALENDAR_ACCOUNT_REQUIRED", errors.New("account_id is required"), false)
		return
	}
	limit := 0
	if value := query.Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_CALENDAR_LIMIT", err, false)
			return
		}
		limit = parsed
	}
	listQuery := calendar.ListQuery{
		CalendarID: query.Get("calendar_id"), Window: calendar.Window(query.Get("window")),
		PageToken: query.Get("page_token"), Limit: limit,
	}
	if value := query.Get("time_min"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_CALENDAR_TIME_MIN", err, false)
			return
		}
		listQuery.TimeMin = parsed
	}
	if value := query.Get("time_max"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_CALENDAR_TIME_MAX", err, false)
			return
		}
		listQuery.TimeMax = parsed
	}
	page, err := a.calendar.ListEvents(request.Context(), scope, accountID, listQuery)
	if err != nil {
		writeDomainError(w, "CALENDAR_EVENTS_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": page.Events, "next_page_token": page.NextPageToken})
}

func (a *application) calendarStoredEventsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	values, err := a.calendar.StoredEvents(request.Context(), scope, strings.TrimSpace(request.URL.Query().Get("account_id")), parseLimit(request.URL.Query().Get("limit"), 25))
	if err != nil {
		writeDomainError(w, "CALENDAR_STORED_EVENTS_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": values})
}

func (a *application) calendarEventRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	accountID := strings.TrimSpace(request.URL.Query().Get("account_id"))
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "CALENDAR_ACCOUNT_REQUIRED", errors.New("account_id is required"), false)
		return
	}
	event, err := a.calendar.GetEvent(request.Context(), scope, accountID, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "CALENDAR_EVENT_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, event)
}

type reminderCreateRequest struct {
	ID             string    `json:"id"`
	Title          string    `json:"title"`
	Body           string    `json:"body,omitempty"`
	DueAt          time.Time `json:"due_at"`
	Timezone       string    `json:"timezone,omitempty"`
	RecurrenceUnit string    `json:"recurrence_unit,omitempty"`
	Interval       int       `json:"interval,omitempty"`
	QuietStart     string    `json:"quiet_start,omitempty"`
	QuietEnd       string    `json:"quiet_end,omitempty"`
	Source         string    `json:"source,omitempty"`
	SourceRef      string    `json:"source_ref,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
}

type reminderApproveRequest struct {
	// ApprovedBy is accepted for backward compatibility but ignored: the
	// approver always comes from the verified request identity.
	ApprovedBy string `json:"approved_by"`
}

func (a *application) reminderList(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	status := reminder.Status(strings.TrimSpace(request.URL.Query().Get("status")))
	values, err := a.reminders.List(request.Context(), scope, reminder.ListQuery{
		Status: status, Limit: parseLimit(request.URL.Query().Get("limit"), reminder.DefaultListLimit),
	})
	if err != nil {
		writeDomainError(w, "REMINDER_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"reminders": values})
}

func (a *application) reminderCreate(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	var body reminderCreateRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	value, err := a.reminders.Create(request.Context(), scope, reminder.Reminder{
		ID: body.ID, Title: body.Title, Body: body.Body, DueAt: body.DueAt, Timezone: body.Timezone,
		Recurrence: reminder.Recurrence{Interval: body.Interval, Unit: reminder.Unit(body.RecurrenceUnit)},
		QuietHours: reminder.QuietHours{Start: body.QuietStart, End: body.QuietEnd},
		Source:     body.Source, SourceRef: body.SourceRef, IdempotencyKey: body.IdempotencyKey,
	})
	if err != nil {
		writeDomainError(w, "REMINDER_CREATE_FAILED", err)
		return
	}
	writeJSON(w, http.StatusCreated, value)
}

func (a *application) reminderRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.reminders.Get(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "REMINDER_READ_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) reminderCancel(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	value, err := a.reminders.Cancel(request.Context(), scope, request.PathValue("id"))
	if err != nil {
		writeDomainError(w, "REMINDER_CANCEL_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) reminderApprove(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	// The approver is the verified request identity; any client-supplied
	// approved_by value is ignored.
	value, err := a.reminders.Approve(request.Context(), scope, request.PathValue("id"), a.resolvedApprover(request))
	if err != nil {
		writeDomainError(w, "REMINDER_APPROVE_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (a *application) reminderTick(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	result, err := a.reminders.Tick(request.Context(), scope)
	if err != nil {
		a.metrics.RecordReminder("error", 1)
		writeDomainError(w, "REMINDER_TICK_FAILED", err)
		return
	}
	a.metrics.RecordReminder("claimed", result.Claimed)
	a.metrics.RecordReminder("delivered", result.Delivered)
	a.metrics.RecordReminder("deferred", result.Deferred)
	a.metrics.RecordReminder("pending_approval", result.PendingApproval)
	writeJSON(w, http.StatusOK, result)
}

func (a *application) inboxRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) {
		return
	}
	items, err := a.reminders.Inbox(request.Context(), scope, parseLimit(request.URL.Query().Get("limit"), reminder.DefaultListLimit))
	if err != nil {
		writeDomainError(w, "INBOX_LIST_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

func (a *application) requireGitHub(w http.ResponseWriter) bool {
	if a.github != nil {
		return true
	}
	writeError(w, http.StatusServiceUnavailable, "GITHUB_NOT_CONFIGURED",
		errors.New("GitHub integration is not configured; set GITHUB_TOKEN and GITHUB_REPOSITORIES"), false)
	return false
}

func githubListQuery(request *http.Request) github.ListQuery {
	query := request.URL.Query()
	page, _ := strconv.Atoi(query.Get("page"))
	return github.ListQuery{
		Repository: query.Get("repository"),
		State:      query.Get("state"),
		HeadSHA:    query.Get("head_sha"),
		Limit:      parseLimit(query.Get("limit"), github.DefaultListLimit),
		Page:       page,
	}
}

func (a *application) githubRepositoryRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) || !a.requireGitHub(w) {
		return
	}
	fullName := request.PathValue("owner") + "/" + request.PathValue("name")
	repository, err := a.github.Repository(request.Context(), scope, fullName)
	if err != nil {
		writeDomainError(w, "GITHUB_REPOSITORY_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, repository)
}

func (a *application) githubIssuesRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) || !a.requireGitHub(w) {
		return
	}
	values, err := a.github.Issues(request.Context(), scope, githubListQuery(request))
	if err != nil {
		writeDomainError(w, "GITHUB_ISSUES_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"issues": values})
}

func (a *application) githubPullRequestsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) || !a.requireGitHub(w) {
		return
	}
	values, err := a.github.PullRequests(request.Context(), scope, githubListQuery(request))
	if err != nil {
		writeDomainError(w, "GITHUB_PULLS_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"pull_requests": values})
}

func (a *application) githubChecksRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) || !a.requireGitHub(w) {
		return
	}
	values, err := a.github.Checks(request.Context(), scope, githubListQuery(request))
	if err != nil {
		writeDomainError(w, "GITHUB_CHECKS_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"checks": values})
}

func (a *application) githubReleasesRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) || !a.requireGitHub(w) {
		return
	}
	values, err := a.github.Releases(request.Context(), scope, githubListQuery(request))
	if err != nil {
		writeDomainError(w, "GITHUB_RELEASES_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"releases": values})
}

func (a *application) githubObjectsRead(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	if !a.requireReady(w) || !a.requireGitHub(w) {
		return
	}
	query := request.URL.Query()
	values, err := a.github.Objects(request.Context(), scope, github.ListObjectsQuery{
		Repository: query.Get("repository"),
		ObjectType: github.ObjectType(strings.TrimSpace(query.Get("object_type"))),
		Limit:      parseLimit(query.Get("limit"), github.DefaultListLimit),
	})
	if err != nil {
		writeDomainError(w, "GITHUB_OBJECTS_FAILED", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"objects": values})
}

func chatSearchQuery(content string) (string, bool) {
	trimmed := strings.TrimSpace(content)
	lower := strings.ToLower(trimmed)
	for _, prefix := range []string{"/search ", "search ", "browse "} {
		if strings.HasPrefix(lower, prefix) {
			query := strings.TrimSpace(trimmed[len(prefix):])
			return query, query != ""
		}
	}
	if strings.HasPrefix(lower, "what changed") || strings.HasPrefix(lower, "what's new") {
		return trimmed, true
	}
	return "", false
}

func formatSearchAnswer(result search.SearchResponse) (string, []chat.Citation) {
	if len(result.Results) == 0 {
		return fmt.Sprintf("I searched for %q but found no results.", result.Query), nil
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "Search results for %q (untrusted external content):\n\n", result.Query)
	citations := make([]chat.Citation, 0, len(result.Results))
	for index, value := range result.Results {
		fmt.Fprintf(&builder, "%d. %s\n%s\n", index+1, value.Title, value.URL)
		if value.Snippet != "" {
			fmt.Fprintf(&builder, "%s\n", value.Snippet)
		}
		builder.WriteString("\n")
		excerpt := value.Snippet
		if content, err := untrusted.New(untrusted.Web, value.Snippet); err == nil {
			excerpt = content.Bounded()
		}
		citations = append(citations, chat.Citation{URL: value.URL, Title: value.Title, Source: value.Source, Excerpt: excerpt, Origin: string(untrusted.Web)})
	}
	return strings.TrimSpace(builder.String()), citations
}

type sourceRequest struct {
	ID                  string   `json:"id,omitempty"`
	Name                string   `json:"name"`
	Kind                string   `json:"kind"`
	URL                 string   `json:"url"`
	Enabled             *bool    `json:"enabled,omitempty"`
	TrustScore          float64  `json:"trust_score,omitempty"`
	PollIntervalSeconds int      `json:"poll_interval_seconds,omitempty"`
	MaxItems            int      `json:"max_items,omitempty"`
	TechnologyHints     []string `json:"technology_hints,omitempty"`
	InterestHints       []string `json:"interest_hints,omitempty"`
}

type sourceResponse struct {
	ID                  string   `json:"id"`
	Name                string   `json:"name"`
	Kind                string   `json:"kind"`
	URL                 string   `json:"url"`
	Enabled             bool     `json:"enabled"`
	TrustScore          float64  `json:"trust_score"`
	PollIntervalSeconds int      `json:"poll_interval_seconds"`
	MaxItems            int      `json:"max_items"`
	TechnologyHints     []string `json:"technology_hints,omitempty"`
	InterestHints       []string `json:"interest_hints,omitempty"`
}

func (a *application) sourcesRead(w http.ResponseWriter, request *http.Request) {
	if !a.requireReady(w) {
		return
	}
	values, err := a.sources.ListSources(request.Context())
	if err != nil {
		writeDomainError(w, "SOURCE_LIST_FAILED", err)
		return
	}
	if id := request.PathValue("id"); id != "" {
		for _, value := range values {
			if value.ID == id {
				writeJSON(w, http.StatusOK, toSourceResponse(value))
				return
			}
		}
		writeError(w, http.StatusNotFound, "SOURCE_NOT_FOUND", errors.New("source not found"), false)
		return
	}
	result := make([]sourceResponse, 0, len(values))
	for _, value := range values {
		result = append(result, toSourceResponse(value))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"sources": result})
}

func (a *application) sourceRead(w http.ResponseWriter, request *http.Request) {
	a.sourcesRead(w, request)
}

func (a *application) sourceSave(w http.ResponseWriter, request *http.Request) {
	if !a.requireReady(w) {
		return
	}
	var body sourceRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	if pathID := request.PathValue("id"); pathID != "" {
		body.ID = pathID
	}
	if body.Kind == "" {
		body.Kind = "rss"
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}
	value := ingestion.SourceConfig{
		ID: body.ID, Name: body.Name, Kind: body.Kind, URL: body.URL, Enabled: enabled,
		TrustScore: body.TrustScore, MaxItems: body.MaxItems,
		TechnologyHints: body.TechnologyHints, InterestHints: body.InterestHints,
	}
	if body.PollIntervalSeconds > 0 {
		value.PollInterval = time.Duration(body.PollIntervalSeconds) * time.Second
	}
	normalized, err := ingestion.NewNormalizer(ingestion.DefaultLimits()).NormalizeSource(value)
	if err != nil {
		writeError(w, http.StatusBadRequest, "SOURCE_INVALID", err, false)
		return
	}
	if _, err := a.adapters.Resolve(normalized); err != nil {
		writeError(w, http.StatusBadRequest, "SOURCE_ADAPTER_UNAVAILABLE", err, false)
		return
	}
	if err := a.sources.UpsertSource(request.Context(), normalized); err != nil {
		writeDomainError(w, "SOURCE_SAVE_FAILED", err)
		return
	}
	status := http.StatusCreated
	if request.Method == http.MethodPut {
		status = http.StatusOK
	}
	writeJSON(w, status, toSourceResponse(normalized))
}

func (a *application) sourceDelete(w http.ResponseWriter, request *http.Request) {
	if !a.requireReady(w) {
		return
	}
	id := request.PathValue("id")
	values, err := a.sources.ListSources(request.Context())
	if err != nil {
		writeDomainError(w, "SOURCE_LIST_FAILED", err)
		return
	}
	found := false
	for _, value := range values {
		if value.ID == id {
			found = true
			break
		}
	}
	if !found {
		writeError(w, http.StatusNotFound, "SOURCE_NOT_FOUND", errors.New("source not found"), false)
		return
	}
	if err := a.sources.DeleteSource(request.Context(), id); err != nil {
		writeDomainError(w, "SOURCE_DELETE_FAILED", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func toSourceResponse(value ingestion.SourceConfig) sourceResponse {
	return sourceResponse{
		ID: value.ID, Name: value.Name, Kind: value.Kind, URL: value.URL, Enabled: value.Enabled,
		TrustScore: value.TrustScore, PollIntervalSeconds: int(value.PollInterval / time.Second),
		MaxItems: value.MaxItems, TechnologyHints: value.TechnologyHints, InterestHints: value.InterestHints,
	}
}

func stableAPIID(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + value))
	return prefix + "_" + fmt.Sprintf("%x", sum[:])[:24]
}

func writeDomainError(w http.ResponseWriter, code string, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, profile.ErrProfileNotFound) || errors.Is(err, project.ErrProjectNotFound) ||
		errors.Is(err, requirement.ErrRequirementNotFound) || errors.Is(err, session.ErrSessionNotFound) ||
		errors.Is(err, session.ErrDecisionNotFound) || errors.Is(err, chat.ErrConversationNotFound) ||
		errors.Is(err, chat.ErrMessageNotFound) || errors.Is(err, mail.ErrAccountNotFound) ||
		errors.Is(err, mail.ErrMessageNotFound) || errors.Is(err, calendar.ErrAccountNotFound) ||
		errors.Is(err, calendar.ErrEventNotFound) || errors.Is(err, reminder.ErrReminderNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, github.ErrRepositoryNotAllowed) || errors.Is(err, github.ErrBlockedOperation) {
		status = http.StatusForbidden
	} else if errors.Is(err, github.ErrProviderUnauthorized) {
		status = http.StatusBadGateway
	} else if strings.Contains(strings.ToLower(err.Error()), "graph") || strings.Contains(strings.ToLower(err.Error()), "falkor") {
		status = http.StatusBadGateway
	}
	writeError(w, status, code, err, status >= http.StatusInternalServerError)
}

func (a *application) run(w http.ResponseWriter, _ *http.Request) {
	if !a.requireReady(w) {
		return
	}
	trigger, err := a.schedule.Trigger()
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, scheduler.ErrNotStarted) || errors.Is(err, scheduler.ErrStopped) {
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, "RUN_NOT_QUEUED", err, status >= http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"status": "queued", "trigger": trigger, "scheduler": schedulerStatus(a.schedule.Status()),
	})
}

func (a *application) schedulerStatus(w http.ResponseWriter, _ *http.Request) {
	_, initializationErr, _ := a.initializationStatus()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"scheduler": schedulerStatus(a.schedule.Status()), "initialization_error": initializationErr,
	})
}

func (a *application) schedulerPause(w http.ResponseWriter, _ *http.Request) {
	if !a.requireReady(w) {
		return
	}
	a.schedule.Pause()
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "paused", "scheduler": schedulerStatus(a.schedule.Status())})
}

func (a *application) schedulerResume(w http.ResponseWriter, _ *http.Request) {
	if !a.requireReady(w) {
		return
	}
	a.schedule.Resume()
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "running", "scheduler": schedulerStatus(a.schedule.Status())})
}

func (a *application) recommendations(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	projectID := request.URL.Query().Get("project_id")
	if projectID == "" {
		projectID = a.config.ProjectID
	}
	items, err := a.graph.ListRecommendations(ctx, scope.UserID, projectID, parseLimit(request.URL.Query().Get("limit"), 10))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "RECOMMENDATIONS_UNAVAILABLE", err, true)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"recommendations": items})
}

func (a *application) graphPath(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	explanation, err := a.tools.Explain(ctx, mcp.ExplainRecommendationInput{
		Meta:             mcp.RequestMeta{TenantID: scope.TenantID, UserID: scope.UserID, RequestID: requestIdentifier(request)},
		RecommendationID: request.PathValue("id"),
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "GRAPH_PATH_NOT_FOUND", err, false)
		return
	}
	writeJSON(w, http.StatusOK, explanation)
}

type feedbackRequest struct {
	Kind mcp.FeedbackKind `json:"kind"`
	Note string           `json:"note,omitempty"`
}

func (a *application) feedback(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	var body feedbackRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	result, err := a.tools.Record(request.Context(), mcp.FeedbackInput{
		Meta:             mcp.RequestMeta{TenantID: scope.TenantID, UserID: scope.UserID, SessionID: request.Header.Get("X-Session-ID"), RequestID: requestIdentifier(request)},
		RecommendationID: request.PathValue("id"), Kind: body.Kind, Note: body.Note,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "FEEDBACK_FAILED", err, false)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type planRequest struct {
	Goal string `json:"goal"`
}

func (a *application) plan(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	var body planRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	if strings.TrimSpace(body.Goal) == "" {
		body.Goal = "Prepare a bounded implementation plan from the graph evidence."
	}
	result, err := a.tools.Propose(request.Context(), mcp.ProposePlanInput{
		Meta:      mcp.RequestMeta{TenantID: scope.TenantID, UserID: scope.UserID, SessionID: request.Header.Get("X-Session-ID"), RequestID: requestIdentifier(request)},
		ProjectID: a.config.ProjectID, RecommendationID: request.PathValue("id"), Goal: body.Goal,
	})
	if err != nil {
		a.metrics.RecordTask("planner", "error")
		writeError(w, http.StatusBadRequest, "PLAN_FAILED", err, false)
		return
	}
	a.metrics.RecordTask("planner", "created")
	writeJSON(w, http.StatusCreated, result)
}

func (a *application) memories(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	items, err := a.memory.Retrieve(ctx, memory.RetrievalQuery{
		TenantID: scope.TenantID, UserID: scope.UserID, ProjectID: request.URL.Query().Get("project_id"),
		Limit: parseLimit(request.URL.Query().Get("limit"), 20), MaxBytes: 64 * 1024,
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "MEMORY_UNAVAILABLE", err, true)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"memories": items})
}

type approvalRequest struct {
	Note string `json:"note,omitempty"`
}

func (a *application) approveAction(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	var body approvalRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	approver, err := auth.ApproverFromRequest(request)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "APPROVAL_IDENTITY_REQUIRED", err, false)
		return
	}
	approved, err := a.actions.Approve(request.Context(), scope.UserID, request.PathValue("id"), actions.Approval{
		ID: requestIdentifier(request), ApprovedBy: approver, Note: body.Note,
	})
	if err != nil {
		a.metrics.RecordApproval("failed")
		writeError(w, http.StatusConflict, "APPROVAL_FAILED", err, false)
		return
	}
	a.metrics.RecordApproval("approved")
	writeJSON(w, http.StatusOK, approved)
}

func (a *application) executeAction(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	result, err := a.actions.Execute(request.Context(), scope.UserID, request.PathValue("id"), a.actionExecutor())
	if err != nil {
		a.metrics.RecordTask("action", "failed")
		writeError(w, http.StatusConflict, "ACTION_FAILED", err, !a.config.ActionsEnabled)
		return
	}
	a.metrics.RecordTask("action", "executed")
	writeJSON(w, http.StatusOK, result)
}

func (a *application) actionExecutor() actions.Executor {
	return actionExecutor{local: localPlanExecutor{dataDir: a.config.DataDir}, github: a.githubExec}
}

// actionExecutor dispatches approved actions to the executor that supports the
// action kind. GitHub writes only reach the provider after the action layer has
// recorded explicit approval.
type actionExecutor struct {
	local  localPlanExecutor
	github *github.Executor
}

func (e actionExecutor) Execute(ctx context.Context, action actions.Action) (actions.ExecutionResult, error) {
	if e.github != nil && e.github.SupportedKind(action.Kind) {
		return e.github.Execute(ctx, action)
	}
	return e.local.Execute(ctx, action)
}

type mcpEnvelope struct {
	Tool  mcp.ToolName    `json:"tool"`
	Input json.RawMessage `json:"input"`
}

func (a *application) mcpTools(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"tools": []map[string]string{
		{"name": string(mcp.SearchRelevantItems), "description": "Search connected project items through a bounded graph query."},
		{"name": string(mcp.ExplainRecommendation), "description": "Return stored graph path and evidence for a recommendation."},
		{"name": string(mcp.RetrieveMemory), "description": "Retrieve bounded typed memories from the user graph."},
		{"name": string(mcp.ProposePlan), "description": "Persist a planner task and approval-gated local plan proposal."},
		{"name": string(mcp.RecordFeedback), "description": "Persist user feedback and its evolving memory."},
	}})
}

func (a *application) mcpInvoke(w http.ResponseWriter, request *http.Request) {
	scope := a.resolvedScope(request)
	var envelope mcpEnvelope
	if err := decodeJSON(w, request, &envelope); err != nil {
		return
	}
	if len(envelope.Input) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", errors.New("input is required"), false)
		return
	}
	meta := mcp.RequestMeta{TenantID: scope.TenantID, UserID: scope.UserID, SessionID: request.Header.Get("X-Session-ID"), RequestID: requestIdentifier(request)}
	var input interface{}
	switch envelope.Tool {
	case mcp.SearchRelevantItems:
		value := mcp.SearchRelevantItemsInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.ExplainRecommendation:
		value := mcp.ExplainRecommendationInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.RetrieveMemory:
		value := mcp.MemoryRetrieveInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.ProposePlan:
		value := mcp.ProposePlanInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.RecordFeedback:
		value := mcp.FeedbackInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	default:
		writeError(w, http.StatusNotFound, "UNKNOWN_TOOL", fmt.Errorf("unknown tool %q", envelope.Tool), false)
		return
	}
	result, err := a.tools.Invoke(request.Context(), envelope.Tool, input)
	if err != nil {
		writeError(w, http.StatusBadRequest, "TOOL_FAILED", err, false)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tool": envelope.Tool, "result": result})
}

type localPlanExecutor struct {
	dataDir string
}

func (e localPlanExecutor) Execute(ctx context.Context, action actions.Action) (actions.ExecutionResult, error) {
	if action.Kind != actions.LocalPlan {
		return actions.ExecutionResult{}, fmt.Errorf("unsupported local executor action kind %q", action.Kind)
	}
	if err := ctx.Err(); err != nil {
		return actions.ExecutionResult{}, err
	}
	planDir := filepath.Join(e.dataDir, "plans")
	if err := os.MkdirAll(planDir, 0o700); err != nil {
		return actions.ExecutionResult{}, err
	}
	goal := action.Payload["goal"]
	recommendationID := action.Payload["recommendation_id"]
	content := fmt.Sprintf("# NightShift implementation plan\n\n- Recommendation: `%s`\n- Project: `%s`\n- Goal: %s\n\nThis plan was generated only after explicit user approval.\n", recommendationID, action.Target, goal)
	path := filepath.Join(planDir, action.ID+".md")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return actions.ExecutionResult{}, err
	}
	return actions.ExecutionResult{Reference: path, Output: "local plan written"}, nil
}

func decodeJSON(w http.ResponseWriter, request *http.Request, destination interface{}) error {
	request.Body = http.MaxBytesReader(w, request.Body, 16*1024)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", err, false)
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		trailing := errors.New("request body must contain one JSON value")
		writeError(w, http.StatusBadRequest, "INVALID_JSON", trailing, false)
		return trailing
	}
	return nil
}

func parseLimit(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return fallback
	}
	if parsed > 50 {
		return 50
	}
	return parsed
}

// identityResolver builds the per-request identity resolver from configuration.
// Exactly one of OIDC or trusted-proxy identity may be configured; otherwise the
// deployment-wide identity is used for loopback-only single-user operation.
func identityResolver(cfg config.Config) (auth.IdentityResolver, error) {
	if cfg.OIDCIssuer != "" {
		return auth.NewOIDCResolver(auth.OIDCConfig{
			Issuer: cfg.OIDCIssuer, Audience: cfg.OIDCAudience, JWKSURL: cfg.OIDCJWKSURL,
			TenantClaim: cfg.OIDCTenantClaim,
		})
	}
	if cfg.TrustedProxySecret != "" {
		return auth.NewTrustedProxyResolver(auth.TrustedProxyConfig{
			Secret:          []byte(cfg.TrustedProxySecret),
			TenantHeader:    cfg.TrustedProxyHeaders.Tenant,
			UserHeader:      cfg.TrustedProxyHeaders.User,
			ApproverHeader:  cfg.TrustedProxyHeaders.Approver,
			SignatureHeader: cfg.TrustedProxyHeaders.Signature,
			TimestampHeader: cfg.TrustedProxyHeaders.Timestamp,
		})
	}
	return nil, nil
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestID := requestIdentifier(request)
		w.Header().Set("X-Request-ID", requestID)
		request = request.WithContext(observability.WithCorrelationID(request.Context(), requestID))
		next.ServeHTTP(w, request)
	})
}

func requestIdentifier(request *http.Request) string {
	if value := request.Header.Get("X-Request-ID"); value != "" {
		return value
	}
	return fmt.Sprintf("req_%d", time.Now().UnixNano())
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("response encode failed", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, code string, err error, retryable bool) {
	writeJSON(w, status, map[string]interface{}{"error": map[string]interface{}{
		"code": code, "message": err.Error(), "request_id": w.Header().Get("X-Request-ID"), "retryable": retryable,
	}})
}

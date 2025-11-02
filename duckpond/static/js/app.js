/**
 * Main Application Logic
 * Handles routing and page rendering with advanced router
 */

/**
 * Router class for client-side routing
 */
class Router {
  constructor() {
    this.routes = [];
    this.currentRoute = null;
    this.beforeHooks = [];
    this.afterHooks = [];
  }

  /**
   * Register a route
   * @param {string} path - Route path (can include :param for dynamic segments)
   * @param {Function} handler - Route handler function
   * @param {Object} options - Route options (auth, title, etc.)
   */
  register(path, handler, options = {}) {
    const pattern = this._pathToRegex(path);
    this.routes.push({
      path,
      pattern,
      handler,
      options,
      paramNames: this._extractParamNames(path),
    });
  }

  /**
   * Convert path to regex pattern
   * @private
   */
  _pathToRegex(path) {
    const pattern = path.replace(/\//g, "\\/").replace(/:(\w+)/g, "([^/]+)");
    return new RegExp(`^${pattern}$`);
  }

  /**
   * Extract parameter names from path
   * @private
   */
  _extractParamNames(path) {
    const matches = path.match(/:(\w+)/g);
    return matches ? matches.map((m) => m.slice(1)) : [];
  }

  /**
   * Match path against registered routes
   * @private
   */
  _matchRoute(path) {
    for (const route of this.routes) {
      const match = path.match(route.pattern);
      if (match) {
        const params = {};
        route.paramNames.forEach((name, index) => {
          params[name] = match[index + 1];
        });
        return { route, params };
      }
    }
    return null;
  }

  /**
   * Add before navigation hook
   * @param {Function} hook - Hook function
   */
  beforeEach(hook) {
    this.beforeHooks.push(hook);
  }

  /**
   * Add after navigation hook
   * @param {Function} hook - Hook function
   */
  afterEach(hook) {
    this.afterHooks.push(hook);
  }

  /**
   * Navigate to a path
   * @param {string} path - Path to navigate to
   * @param {boolean} replace - Replace history instead of push
   */
  async navigate(path, replace = false) {
    // Run before hooks
    for (const hook of this.beforeHooks) {
      const result = await hook(path, this.currentRoute);
      if (result === false) {
        return; // Navigation cancelled
      }
    }

    // Update browser history
    if (replace) {
      window.history.replaceState({}, "", path);
    } else {
      window.history.pushState({}, "", path);
    }

    // Handle the route
    await this._handleRoute(path);

    // Run after hooks
    for (const hook of this.afterHooks) {
      await hook(path, this.currentRoute);
    }
  }

  /**
   * Handle route changes
   * @private
   */
  async _handleRoute(path) {
    const matched = this._matchRoute(path);

    if (matched) {
      const { route, params } = matched;
      this.currentRoute = { path, params, options: route.options };

      try {
        await route.handler(params);
      } catch (error) {
        console.error("Route handler error:", error);
        this._handleError(error);
      }
    } else {
      this._handle404(path);
    }
  }

  /**
   * Handle 404 errors
   * @private
   */
  _handle404(path) {
    console.warn("Route not found:", path);
    this.navigate("/app", true);
  }

  /**
   * Handle route errors
   * @private
   */
  _handleError(error) {
    console.error("Routing error:", error);
    // Could show error page here
  }

  /**
   * Initialize the router
   */
  init() {
    // Handle browser back/forward
    window.addEventListener("popstate", () => {
      this._handleRoute(window.location.pathname);
    });

    // Initial route
    this._handleRoute(window.location.pathname);
  }
}

/**
 * Application class
 */
class App {
  constructor() {
    this.router = new Router();
    this.container = null;
    this.isLoading = false;
    this.currentView = null;
  }

  /**
   * Initialize the application
   */
  init() {
    this.container = document.getElementById("content-container");

    // Register routes
    this._registerRoutes();

    // Add route guards
    this._setupRouteGuards();

    // Initialize router
    this.router.init();
  }

  /**
   * Register all application routes
   * @private
   */
  _registerRoutes() {
    // Redirect /app to notebooks
    this.router.register(
      "/app",
      () => {
        window.location.href = "/app/notebooks";
      },
      {
        requireAuth: true,
        title: "DuckPond",
      },
    );

    // Notebooks list
    this.router.register("/app/notebooks", () => this.renderNotebooks(), {
      requireAuth: true,
      title: "Notebooks - DuckPond",
    });

    // Notebook detail
    this.router.register(
      "/app/notebooks/:id",
      (params) => this.renderNotebookDetail(params.id),
      {
        requireAuth: true,
        title: "Notebook - DuckPond",
      },
    );

    // Settings
    this.router.register("/app/settings", () => this.renderSettings(), {
      requireAuth: true,
      title: "Settings - DuckPond",
    });
  }

  /**
   * Setup route guards (authentication, etc.)
   * @private
   */
  _setupRouteGuards() {
    // Before each route
    this.router.beforeEach(async (to, from) => {
      const route = this.router._matchRoute(to);

      // Check authentication
      if (route?.route.options.requireAuth) {
        const isAuth = window.authManager.isAuthenticated();
        if (!isAuth) {
          window.authManager.redirectToLogin();
          return false; // Cancel navigation
        }
      }

      // Show loading state
      this._showLoading();

      return true; // Continue navigation
    });

    // After each route
    this.router.afterEach(async (to) => {
      const route = this.router._matchRoute(to);

      // Update page title
      if (route?.route.options.title) {
        document.title = route.route.options.title;
      }

      // Hide loading state
      this._hideLoading();

      // Scroll to top
      window.scrollTo(0, 0);
    });
  }

  /**
   * Show loading state
   * @private
   */
  _showLoading() {
    if (this.isLoading) return;
    this.isLoading = true;

    const loadingScreen = this.container.querySelector(".loading-screen");
    if (!loadingScreen) {
      const loading = document.createElement("div");
      loading.className = "loading-screen";
      loading.innerHTML = `
                <div class="loading-spinner-large"></div>
                <p>Loading...</p>
            `;
      this.container.appendChild(loading);
    }
  }

  /**
   * Hide loading state
   * @private
   */
  _hideLoading() {
    this.isLoading = false;
    const loadingScreen = this.container.querySelector(".loading-screen");
    if (loadingScreen) {
      loadingScreen.remove();
    }
  }

  /**
   * Navigate to a route by name
   * @param {string} routeName - Route name (notebooks, settings)
   * @param {Object} params - Route parameters
   */
  navigate(routeName, params = {}) {
    const routes = {
      notebooks: "/app/notebooks",
      "notebook-detail": `/app/notebooks/${params.id}`,
      settings: "/app/settings",
    };

    const path = routes[routeName] || routes.notebooks;
    this.router.navigate(path);
  }

  /**
   * Render notebooks view
   */
  async renderNotebooks() {
    // Cleanup previous view if exists
    if (this.currentView && this.currentView.destroy) {
      this.currentView.destroy();
    }

    if (window.NotebooksView) {
      this.currentView = new window.NotebooksView();
      await this.currentView.render(this.container);
    } else {
      // Fallback if NotebooksView not loaded
      this.currentView = null;
      this.container.innerHTML = `
                <div class="page-header">
                    <h1>Notebooks</h1>
                    <p class="text-muted">Manage your Marimo notebooks</p>
                </div>
                <div class="info-card">
                    <h3>📓 Loading Notebooks...</h3>
                    <p>Notebooks view is being loaded.</p>
                </div>
            `;
    }
  }

  /**
   * Render notebook detail view
   * @param {string} notebookId - Notebook ID
   */
  async renderNotebookDetail(notebookId) {
    this.container.innerHTML = `
            <div class="page-header">
                <div style="display: flex; align-items: center; gap: 16px;">
                    <button onclick="window.app.navigate('notebooks')" class="back-button">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="19" y1="12" x2="5" y2="12"></line>
                            <polyline points="12 19 5 12 12 5"></polyline>
                        </svg>
                    </button>
                    <div>
                        <h1>Notebook: ${notebookId}</h1>
                        <p class="text-muted">Notebook details and management</p>
                    </div>
                </div>
            </div>
            <div class="notebook-detail-content">
                <div class="info-card">
                    <h3>📓 Notebook Detail View</h3>
                    <p>This page will display:</p>
                    <ul>
                        <li>Notebook metadata and status</li>
                        <li>Start/stop controls</li>
                        <li>Open in new tab button</li>
                        <li>Session information</li>
                        <li>Delete notebook option</li>
                    </ul>
                    <p style="margin-top: 16px;">
                        <strong>Notebook ID:</strong> <code>${notebookId}</code>
                    </p>
                </div>
            </div>
        `;
  }

  /**
   * Render settings view
   */
  async renderSettings() {
    // Cleanup previous view if exists
    if (this.currentView && this.currentView.destroy) {
      this.currentView.destroy();
    }

    if (window.SettingsView) {
      this.currentView = new window.SettingsView();
      await this.currentView.render(this.container);
    } else {
      // Fallback if SettingsView not loaded
      this.currentView = null;
      this.container.innerHTML = `
                <div class="page-header">
                    <h1>Settings</h1>
                    <p class="text-muted">Manage your account settings</p>
                </div>
                <div class="info-card">
                    <h3>⚙️ Loading Settings...</h3>
                    <p>Settings view is being loaded.</p>
                </div>
            `;
    }
  }
}

// Create global app instance
if (typeof window !== "undefined") {
  window.app = new App();
}

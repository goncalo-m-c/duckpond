/**
 * Notebooks List View
 * Displays all notebooks with search, filter, and CRUD operations
 */

class NotebooksView {
  constructor() {
    this.container = null;
    this.data = {
      notebooks: [],
      sessions: [],
      filteredNotebooks: [],
      searchQuery: "",
      filterStatus: "all",
      sortBy: "name",
      sortOrder: "desc",
    };
    this.pollInterval = null;
    this.pollIntervalMs = 10000; // 10 seconds (faster for sessions)
  }

  /**
   * Render the notebooks view
   * @param {HTMLElement} container - Container element
   */
  async render(container) {
    this.container = container;

    // Show loading state with skeleton
    this._showLoading();

    try {
      // Fetch notebooks
      await this._fetchNotebooks();

      // Render view
      this._renderNotebooks();

      // Start polling for session updates
      this._startPolling();
    } catch (error) {
      console.error("Notebooks render error:", error);
      this._renderError(error);
      toast.error("Failed to load notebooks");
    }
  }

  /**
   * Cleanup when leaving view
   */
  destroy() {
    this._stopPolling();
  }

  /**
   * Fetch notebooks from API
   * @param {boolean} silent - Silent refresh for polling
   * @private
   */
  async _fetchNotebooks(silent = false) {
    try {
      const response = await window.api.listNotebooks();
      this.data.notebooks = Array.isArray(response) ? response : [];

      // Fetch sessions and merge with notebooks
      await this._fetchSessions(silent);
      this._mergeSessionsWithNotebooks();

      this._applyFilters();
    } catch (error) {
      console.error("Failed to fetch notebooks:", error);
      if (!silent) {
        throw error;
      }
    }
  }

  /**
   * Fetch active sessions from API
   * @param {boolean} silent - Silent refresh for polling
   * @private
   */
  async _fetchSessions(silent = false) {
    try {
      const sessions = await window.api.listSessions();
      this.data.sessions = Array.isArray(sessions) ? sessions : [];
    } catch (error) {
      console.error("Failed to fetch sessions:", error);
      if (!silent) {
        throw error;
      }
    }
  }

  /**
   * Merge session data with notebooks
   * @private
   */
  _mergeSessionsWithNotebooks() {
    this.data.notebooks = this.data.notebooks.map((notebook) => {
      // Find matching session by notebook_path
      const session = this.data.sessions.find(
        (s) =>
          s.notebook_path === notebook.filename ||
          s.notebook_path === notebook.path ||
          s.notebook_path.endsWith(notebook.filename),
      );

      if (session) {
        // Construct UI URL from session_id since SessionInfoResponse doesn't include it
        const ui_url = `/notebooks/sessions/${session.session_id}/ui`;
        return {
          ...notebook,
          session_id: session.session_id,
          status: session.status,
          url: ui_url,
        };
      }

      return {
        ...notebook,
        session_id: null,
        status: "stopped",
        url: null,
      };
    });
  }

  /**
   * Apply search and filter to notebooks
   * @private
   */
  _applyFilters() {
    let filtered = [...this.data.notebooks];

    // Apply search
    if (this.data.searchQuery) {
      const query = this.data.searchQuery.toLowerCase();
      filtered = filtered.filter((n) =>
        (n.filename || "").toLowerCase().includes(query),
      );
    }

    // Apply status filter
    if (this.data.filterStatus !== "all") {
      filtered = filtered.filter((n) => {
        if (this.data.filterStatus === "active") {
          return n.status === "running";
        } else if (this.data.filterStatus === "idle") {
          return n.status !== "running";
        }
        return true;
      });
    }

    // Apply sorting by name only
    filtered.sort((a, b) => {
      const aVal = (a.filename || "").toLowerCase();
      const bVal = (b.filename || "").toLowerCase();

      if (this.data.sortOrder === "asc") {
        return aVal > bVal ? 1 : -1;
      } else {
        return aVal < bVal ? 1 : -1;
      }
    });

    this.data.filteredNotebooks = filtered;
  }

  /**
   * Start polling for updates
   * @private
   */
  _startPolling() {
    this._stopPolling();

    this.pollInterval = setInterval(async () => {
      try {
        await this._fetchNotebooks(true);
        // Only update the notebook cards, not the whole view
        this._updateNotebookCards();
      } catch (error) {
        console.error("Poll update failed:", error);
      }
    }, this.pollIntervalMs);

    console.log(
      `Notebooks polling started (every ${this.pollIntervalMs / 1000}s)`,
    );
  }

  /**
   * Stop polling
   * @private
   */
  _stopPolling() {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
      console.log("Notebooks polling stopped");
    }
  }

  /**
   * Show loading state
   * @private
   */
  _showLoading() {
    this.container.innerHTML = `
      <div class="loading-screen">
        <div class="loading-spinner loading-spinner-large"></div>
        <p class="loading-text">Loading notebooks...</p>
      </div>
    `;
  }

  /**
   * Show error state
   * @private
   */
  _renderError(error) {
    this.container.innerHTML = "";
    const errorDisplay = ErrorDisplay.create(error, {
      title: "Failed to load notebooks",
      showRetry: true,
      onRetry: () => {
        this.render(this.container);
      },
    });
    this.container.appendChild(errorDisplay);
  }

  /**
   * Render notebooks view
   * @private
   */
  _renderNotebooks() {
    const notebooks = this.data.filteredNotebooks;

    this.container.innerHTML = `
            <div class="notebooks-view">
                <!-- Header -->
                <div class="notebooks-header">
                    <h1>Notebooks</h1>
                    <button class="btn-primary" id="create-notebook-btn">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="12" y1="5" x2="12" y2="19"></line>
                            <line x1="5" y1="12" x2="19" y2="12"></line>
                        </svg>
                        New Notebook
                    </button>
                </div>

                <!-- Search and Filter Bar -->
                <div class="notebooks-controls">
                    <div class="search-box">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="11" cy="11" r="8"></circle>
                            <path d="m21 21-4.35-4.35"></path>
                        </svg>
                        <input
                            type="text"
                            id="search-input"
                            placeholder="Search notebooks..."
                            value="${this.data.searchQuery}"
                        />
                    </div>

                    <div class="filter-controls">
                        <select id="status-filter" class="filter-select">
                            <option value="all" ${this.data.filterStatus === "all" ? "selected" : ""}>All Notebooks</option>
                            <option value="active" ${this.data.filterStatus === "active" ? "selected" : ""}>Active Sessions</option>
                            <option value="idle" ${this.data.filterStatus === "idle" ? "selected" : ""}>Idle</option>
                        </select>

                        <select id="sort-select" class="filter-select">
                            <option value="name" ${this.data.sortBy === "name" ? "selected" : ""}>Sort by Name</option>
                        </select>
                    </div>
                </div>

                <!-- Notebooks List -->
                <div class="notebooks-list" id="notebooks-list">
                    ${notebooks.length > 0 ? this._renderNotebookCards() : this._renderEmptyState()}
                </div>
            </div>

            <!-- Create Notebook Modal -->
            <div class="modal" id="create-modal">
                <div class="modal-overlay"></div>
                <div class="modal-content">
                    <div class="modal-header">
                        <h2>Create New Notebook</h2>
                        <button class="modal-close" id="modal-close">
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    </div>
                    <div class="modal-body">
                        <div class="form-group">
                            <label for="notebook-name">Notebook Name</label>
                            <input
                                type="text"
                                id="notebook-name"
                                placeholder="my_notebook.py"
                                autocomplete="off"
                            />
                            <span class="input-hint">Must end with .py</span>
                        </div>
                        <div id="create-error" class="error-message hidden"></div>
                    </div>
                    <div class="modal-footer">
                        <button class="btn-secondary" id="modal-cancel">Cancel</button>
                        <button class="btn-primary" id="modal-create">Create & Open</button>
                    </div>
                </div>
            </div>


        `;

    // Attach event listeners
    this._attachEventListeners();
  }

  /**
   * Render notebook cards
   * @private
   */
  _renderNotebookCards() {
    return this.data.filteredNotebooks
      .map(
        (notebook) => `
            <div class="notebook-card" data-notebook-id="${notebook.filename}">
                <div class="notebook-icon">
                    ${notebook.status === "running" ? "🟢" : "📓"}
                </div>
                <div class="notebook-content">
                    <div class="notebook-title">${notebook.filename || "Untitled"}</div>
                    <div class="notebook-meta">
                        <span class="notebook-modified">
                            Modified ${window.utils.formatRelativeTime(notebook.modified_at)}
                        </span>
                        ${
                          notebook.size_bytes
                            ? `
                            <span class="notebook-size">
                                ${window.utils.formatBytes(notebook.size_bytes)}
                            </span>
                        `
                            : ""
                        }
                        <span class="notebook-status status-${notebook.status || "stopped"}">
                            ${this._getStatusText(notebook.status)}
                        </span>
                    </div>
                </div>
                <div class="notebook-actions">
                    ${
                      notebook.status === "running"
                        ? `
                        <button class="btn-secondary" data-action="open" data-notebook-id="${notebook.filename}">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                                <polyline points="15 3 21 3 21 9"></polyline>
                                <line x1="10" y1="14" x2="21" y2="3"></line>
                            </svg>
                            Open
                        </button>
                        <button class="btn-secondary" data-action="stop" data-notebook-id="${notebook.filename}">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="6" y="6" width="12" height="12"></rect>
                            </svg>
                            Stop
                        </button>
                    `
                        : `
                        <button class="btn-secondary" data-action="start" data-notebook-id="${notebook.filename}">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polygon points="5 3 19 12 5 21 5 3"></polygon>
                            </svg>
                            Start
                        </button>
                    `
                    }
                    <button class="btn-danger-outline" data-action="delete" data-notebook-id="${notebook.filename}" data-notebook-name="${notebook.filename}">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="3 6 5 6 21 6"></polyline>
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                        </svg>
                        Delete
                    </button>
                </div>
            </div>
        `,
      )
      .join("");
  }

  /**
   * Update notebook cards without full re-render
   * @private
   */
  _updateNotebookCards() {
    const listContainer = this.container.querySelector("#notebooks-list");
    if (!listContainer) return;

    this._applyFilters();

    if (this.data.filteredNotebooks.length > 0) {
      listContainer.innerHTML = this._renderNotebookCards();
      // Re-attach event listeners for notebook cards
      this._attachNotebookCardListeners();
    }
  }

  /**
   * Render empty state
   * @private
   */
  _renderEmptyState() {
    if (this.data.searchQuery) {
      // Search returned no results
      const emptyState = EmptyState.create({
        icon: "🔍",
        title: "No notebooks found",
        message: `No notebooks match "${this.data.searchQuery}". Try a different search term.`,
      });
      return emptyState.outerHTML;
    } else {
      // No notebooks at all
      const emptyState = EmptyState.create({
        icon: "📓",
        title: "No notebooks yet",
        message:
          "Create your first notebook to get started with interactive data analysis.",
        actionText: "Create Notebook",
        actionLink: "#",
      });

      // Need to handle the click since it's just a # link
      setTimeout(() => {
        const actionBtn = this.container.querySelector(".empty-state-action");
        if (actionBtn) {
          actionBtn.addEventListener("click", (e) => {
            e.preventDefault();
            this._showCreateModal();
          });
        }
      }, 0);

      return emptyState.outerHTML;
    }
  }

  /**
   * Get status text
   * @private
   */
  _getStatusText(status) {
    const statusMap = {
      running: "● Running",
      starting: "◐ Starting",
      stopped: "○ Stopped",
      error: "✕ Error",
    };
    return statusMap[status] || "○ Unknown";
  }

  /**
   * Attach event listeners
   * @private
   */
  _attachEventListeners() {
    // Create button
    const createBtn = this.container.querySelector("#create-notebook-btn");
    const createBtnEmpty = this.container.querySelector(
      "#create-notebook-empty",
    );

    if (createBtn) {
      createBtn.addEventListener("click", () => this._showCreateModal());
    }
    if (createBtnEmpty) {
      createBtnEmpty.addEventListener("click", () => this._showCreateModal());
    }

    // Search input
    const searchInput = this.container.querySelector("#search-input");
    if (searchInput) {
      searchInput.addEventListener(
        "input",
        window.utils.debounce((e) => {
          this.data.searchQuery = e.target.value;
          this._applyFilters();
          this._updateNotebookCards();
        }, 300),
      );
    }

    // Filter select
    const statusFilter = this.container.querySelector("#status-filter");
    if (statusFilter) {
      statusFilter.addEventListener("change", (e) => {
        this.data.filterStatus = e.target.value;
        this._applyFilters();
        this._updateNotebookCards();
      });
    }

    // Sort select
    const sortSelect = this.container.querySelector("#sort-select");
    if (sortSelect) {
      sortSelect.addEventListener("change", (e) => {
        this.data.sortBy = e.target.value;
        this._applyFilters();
        this._updateNotebookCards();
      });
    }

    // Create modal listeners
    this._attachModalListeners();

    // Notebook card listeners
    this._attachNotebookCardListeners();
  }

  /**
   * Attach notebook card listeners
   * @private
   */
  _attachNotebookCardListeners() {
    const notebookCards = this.container.querySelectorAll(".notebook-card");

    notebookCards.forEach((card) => {
      // Click on card (except actions) navigates to detail
      card.addEventListener("click", (e) => {
        if (!e.target.closest(".notebook-actions")) {
          const notebookId = card.dataset.notebookId;
          window.app.navigate("notebook-detail", { id: notebookId });
        }
      });

      // Action buttons
      const actionButtons = card.querySelectorAll("[data-action]");
      actionButtons.forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const action = btn.dataset.action;
          const notebookId = btn.dataset.notebookId;

          switch (action) {
            case "start":
              this._startNotebook(notebookId);
              break;
            case "open":
              this._openNotebook(notebookId);
              break;
            case "stop":
              this._stopNotebook(notebookId);
              break;
            case "delete":
              this._deleteNotebook(notebookId, btn.dataset.notebookName);
              break;
          }
        });
      });
    });
  }

  /**
   * Attach modal listeners
   * @private
   */
  _attachModalListeners() {
    // Create modal
    const createModal = this.container.querySelector("#create-modal");
    const modalClose = this.container.querySelector("#modal-close");
    const modalCancel = this.container.querySelector("#modal-cancel");
    const modalCreate = this.container.querySelector("#modal-create");
    const notebookNameInput = this.container.querySelector("#notebook-name");

    if (modalClose) {
      modalClose.addEventListener("click", () => this._hideModal("create"));
    }
    if (modalCancel) {
      modalCancel.addEventListener("click", () => this._hideModal("create"));
    }
    if (modalCreate) {
      modalCreate.addEventListener("click", () => this._createNotebook());
    }
    if (notebookNameInput) {
      notebookNameInput.addEventListener("keypress", (e) => {
        if (e.key === "Enter") {
          this._createNotebook();
        }
      });
    }

    // Click outside modal to close
    const modals = this.container.querySelectorAll(".modal");
    modals.forEach((modal) => {
      const overlay = modal.querySelector(".modal-overlay");
      if (overlay) {
        overlay.addEventListener("click", () => {
          const modalId = modal.id.replace("-modal", "");
          this._hideModal(modalId);
        });
      }
    });
  }

  /**
   * Show create modal
   * @private
   */
  _showCreateModal() {
    const modal = this.container.querySelector("#create-modal");
    const input = this.container.querySelector("#notebook-name");
    const error = this.container.querySelector("#create-error");

    if (modal) {
      modal.classList.add("active");
      if (input) {
        input.value = "";
        input.focus();
      }
      if (error) {
        error.classList.add("hidden");
      }
    }
  }

  /**
   * Show delete confirmation modal
   * @private
   */

  /**
   * Hide modal
   * @private
   */
  _hideModal(modalType) {
    const modal = this.container.querySelector(`#${modalType}-modal`);
    if (modal) {
      modal.classList.remove("active");
    }
  }

  /**
   * Create notebook
   * @private
   */
  async _createNotebook() {
    const input = this.container.querySelector("#notebook-name");
    const error = this.container.querySelector("#create-error");
    const createBtn = this.container.querySelector("#modal-create");

    const name = input.value.trim();

    // Validation
    if (!name) {
      this._showModalError("Please enter a notebook name");
      return;
    }

    if (!name.endsWith(".py")) {
      this._showModalError("Notebook name must end with .py");
      return;
    }

    // Show loading state on button
    ButtonLoadingState.show(createBtn, "Creating...");

    try {
      const response = await window.api.createNotebook({ filename: name });

      // Refresh notebooks list
      await this._fetchNotebooks();
      this._renderNotebooks();

      // Close modal
      this._hideModal("create");

      // Show success message
      toast.success(`Notebook "${name}" created successfully`);

      // Navigate to new notebook
      if (response.filename) {
        window.app.navigate("notebook-detail", { id: response.filename });
      }
    } catch (error) {
      console.error("Failed to create notebook:", error);
      this._showModalError(error.message || "Failed to create notebook");
      ButtonLoadingState.hide(createBtn);
      toast.error("Failed to create notebook");
    }
  }

  /**
   * Delete notebook (deprecated - now using Modal.confirm)
   * @private
   */
  async _deleteNotebook(notebookId, notebookName) {
    try {
      await window.api.deleteNotebook(notebookId);

      toast.success(`Notebook "${notebookName}" deleted successfully`);

      // Refresh list
      await this._fetchNotebooks();
      this._renderNotebooks();
    } catch (error) {
      console.error("Failed to delete notebook:", error);
      toast.error("Failed to delete notebook: " + error.message);
    }
  }

  /**
   * Show modal error
   * @private
   */
  _showModalError(message) {
    const error = this.container.querySelector("#create-error");
    if (error) {
      error.textContent = message;
      error.classList.remove("hidden");
    }
  }

  /**
   * Start notebook (create session)
   * @private
   */
  async _startNotebook(notebookId) {
    // Show progress indicator for session startup
    const progressSteps = [
      "Creating session...",
      "Starting container...",
      "Waiting for marimo...",
      "Ready!",
    ];

    const progressIndicator = new ProgressIndicator(progressSteps);
    const progressElement = progressIndicator.create();

    // Find the notebook card and show progress
    const card = this.container.querySelector(
      `[data-notebook-id="${notebookId}"]`,
    );
    if (card) {
      const actionsDiv = card.querySelector(".notebook-actions");
      const originalContent = actionsDiv.innerHTML;
      actionsDiv.innerHTML = "";
      actionsDiv.appendChild(progressElement);
      progressIndicator.setStep(0);
    }

    try {
      // Step 1: Create session
      progressIndicator.setStep(0);
      const response = await window.api.createSession({
        notebook_path: notebookId,
      });

      console.log("Session created:", response);

      // Step 2: Container starting (simulate with delay)
      progressIndicator.setStep(1);
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Step 3: Waiting for marimo
      progressIndicator.setStep(2);
      await new Promise((resolve) => setTimeout(resolve, 1500));

      // Step 4: Ready
      progressIndicator.setStep(3);
      progressIndicator.complete();

      toast.success(`Notebook "${notebookId}" is now running`);

      // Refresh to show updated status
      await this._fetchNotebooks(true);
      this._updateNotebookCards();
    } catch (error) {
      console.error("Failed to start notebook:", error);
      progressIndicator.error(0);
      toast.error("Failed to start notebook: " + error.message);

      // Restore original content after error
      if (card) {
        const actionsDiv = card.querySelector(".notebook-actions");
        if (actionsDiv && originalContent) {
          setTimeout(() => {
            actionsDiv.innerHTML = originalContent;
            this._attachNotebookCardListeners();
          }, 2000);
        }
      }
    }
  }

  /**
   * Open notebook
   * @private
   */
  _openNotebook(notebookId) {
    const notebook = this.data.notebooks.find((n) => n.filename === notebookId);
    if (notebook && notebook.url) {
      window.open(notebook.url, "_blank");
    }
  }

  /**
   * Stop notebook (terminate session)
   * @private
   */
  async _stopNotebook(notebookId) {
    const notebook = this.data.notebooks.find((n) => n.filename === notebookId);

    if (!notebook || !notebook.session_id) {
      toast.warning("No active session found for this notebook");
      return;
    }

    // Show progress indicator for session shutdown
    const progressSteps = ["Stopping session...", "Cleaning up...", "Stopped!"];

    const progressIndicator = new ProgressIndicator(progressSteps);
    const progressElement = progressIndicator.create();

    // Find the notebook card and show progress
    const card = this.container.querySelector(
      `[data-notebook-id="${notebookId}"]`,
    );
    let originalContent = null;
    if (card) {
      const actionsDiv = card.querySelector(".notebook-actions");
      originalContent = actionsDiv.innerHTML;
      actionsDiv.innerHTML = "";
      actionsDiv.appendChild(progressElement);
      progressIndicator.setStep(0);
    }

    try {
      // Step 1: Stop session
      progressIndicator.setStep(0);
      await window.api.terminateSession(notebook.session_id);

      console.log("Session terminated:", notebook.session_id);

      // Step 2: Cleaning up
      progressIndicator.setStep(1);
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Step 3: Stopped
      progressIndicator.setStep(2);
      progressIndicator.complete();

      toast.success(`Session stopped for "${notebookId}"`);

      // Refresh to show updated status
      await this._fetchNotebooks(true);
      this._updateNotebookCards();
    } catch (error) {
      console.error("Failed to stop notebook:", error);
      progressIndicator.error(0);
      toast.error("Failed to stop notebook: " + error.message);

      // Restore original content after error
      if (card && originalContent) {
        const actionsDiv = card.querySelector(".notebook-actions");
        if (actionsDiv) {
          setTimeout(() => {
            actionsDiv.innerHTML = originalContent;
            this._attachNotebookCardListeners();
          }, 2000);
        }
      }
    }
  }
}

// Export for use in app.js
if (typeof window !== "undefined") {
  window.NotebooksView = NotebooksView;
}

/**
 * API Client for DuckPond
 * Provides methods for interacting with the backend API
 */

class APIClient {
  constructor(baseURL = "") {
    this.baseURL = baseURL;
  }

  /**
   * Make an authenticated API request
   * @param {string} endpoint - API endpoint
   * @param {Object} options - Fetch options
   * @returns {Promise<any>} Response data
   */
  async request(endpoint, options = {}) {
    const url = `${this.baseURL}${endpoint}`;
    const response = await authenticatedFetch(url, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...options.headers,
      },
    });

    if (!response.ok) {
      const error = await response
        .json()
        .catch(() => ({ detail: "Request failed" }));
      throw new Error(
        error.detail || `Request failed with status ${response.status}`,
      );
    }

    return response.json();
  }

  /**
   * GET request
   * @param {string} endpoint - API endpoint
   * @param {Object} params - Query parameters
   * @returns {Promise<any>} Response data
   */
  async get(endpoint, params = {}) {
    const queryString = new URLSearchParams(params).toString();
    const url = queryString ? `${endpoint}?${queryString}` : endpoint;
    return this.request(url, { method: "GET" });
  }

  /**
   * POST request
   * @param {string} endpoint - API endpoint
   * @param {Object} data - Request body
   * @returns {Promise<any>} Response data
   */
  async post(endpoint, data = {}) {
    return this.request(endpoint, {
      method: "POST",
      body: JSON.stringify(data),
    });
  }

  /**
   * PUT request
   * @param {string} endpoint - API endpoint
   * @param {Object} data - Request body
   * @returns {Promise<any>} Response data
   */
  async put(endpoint, data = {}) {
    return this.request(endpoint, {
      method: "PUT",
      body: JSON.stringify(data),
    });
  }

  /**
   * PATCH request
   * @param {string} endpoint - API endpoint
   * @param {Object} data - Request body
   * @returns {Promise<any>} Response data
   */
  async patch(endpoint, data = {}) {
    return this.request(endpoint, {
      method: "PATCH",
      body: JSON.stringify(data),
    });
  }

  /**
   * DELETE request
   * @param {string} endpoint - API endpoint
   * @returns {Promise<any>} Response data
   */
  async delete(endpoint) {
    return this.request(endpoint, { method: "DELETE" });
  }

  // Datasets API
  async listDatasets(params = {}) {
    return this.get("/api/datasets", params);
  }

  async getDataset(datasetId) {
    return this.get(`/api/datasets/${datasetId}`);
  }

  async deleteDataset(datasetId) {
    return this.delete(`/api/datasets/${datasetId}`);
  }

  // Notebooks API - File Management
  async listNotebooks() {
    return this.get("/notebooks/files");
  }

  async getNotebook(filename) {
    return this.get(`/notebooks/files/${filename}`);
  }

  async createNotebook(data) {
    return this.post("/notebooks/files", data);
  }

  async updateNotebook(filename, data) {
    return this.put(`/notebooks/files/${filename}`, data);
  }

  async deleteNotebook(filename) {
    return this.delete(`/notebooks/files/${filename}`);
  }

  // Notebooks API - Session Management
  async listSessions() {
    return this.get("/notebooks/sessions");
  }

  async getSession(sessionId) {
    return this.get(`/notebooks/sessions/${sessionId}`);
  }

  async createSession(data) {
    return this.post("/notebooks/sessions", data);
  }

  async terminateSession(sessionId) {
    return this.delete(`/notebooks/sessions/${sessionId}`);
  }

  async getNotebookStatus() {
    return this.get("/notebooks/status");
  }

  // Query API
  async executeQuery(query, params = {}) {
    return this.post("/api/query", { query, ...params });
  }

  // Upload API
  async uploadFile(file, datasetName) {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("dataset_name", datasetName);

    const response = await authenticatedFetch("/api/upload", {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const error = await response
        .json()
        .catch(() => ({ detail: "Upload failed" }));
      throw new Error(error.detail || "Upload failed");
    }

    return response.json();
  }

  // Health check
  async healthCheck() {
    const response = await fetch("/health");
    return response.json();
  }
}

// Create global API client instance
if (typeof window !== "undefined") {
  window.api = new APIClient();
}

// Export for module systems
if (typeof module !== "undefined" && module.exports) {
  module.exports = { APIClient };
}

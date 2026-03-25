// API.js - Client-side API functions for Financial Audit Anomaly Detection

const API_BASE_URL = '/api/';

/**
 * Generic GET request
 * @param {string} endpoint - API endpoint
 * @returns {Promise<Object>} Response data
 */
async function apiGet(endpoint) {
  try {
    const response = await fetch(`${API_BASE_URL}${endpoint}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('API GET error:', error);
    throw error;
  }
}

/**
 * Generic POST request
 * @param {string} endpoint - API endpoint
 * @param {Object} data - Data to send
 * @returns {Promise<Object>} Response data
 */
async function apiPost(endpoint, data) {
  try {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('API POST error:', error);
    throw error;
  }
}

/**
 * Fetch transactions data
 * @param {Object} filters - Optional filters
 * @returns {Promise<Array>} Transactions array
 */
async function getTransactions(filters = {}) {
  const query = new URLSearchParams(filters).toString();
  const endpoint = `transactions${query ? '?' + query : ''}`;
  return apiGet(endpoint);
}

/**
 * Fetch anomalies data
 * @param {Object} filters - Optional filters
 * @returns {Promise<Array>} Anomalies array
 */
async function getAnomalies(filters = {}) {
  const query = new URLSearchParams(filters).toString();
  const endpoint = `anomalies${query ? '?' + query : ''}`;
  return apiGet(endpoint);
}

/**
 * Upload audit data
 * @param {FormData} formData - File data
 * @returns {Promise<Object>} Upload result
 */
async function uploadAuditData(formData) {
  try {
    const response = await fetch(`${API_BASE_URL}upload`, {
      method: 'POST',
      body: formData,
    });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('Upload error:', error);
    throw error;
  }
}

/**
 * Get dashboard KPIs
 * @returns {Promise<Object>} KPI data
 */
async function getDashboardKPIs() {
  return apiGet('dashboard/kpis');
}

/**
 * Get audit reports
 * @param {string} auditId - Optional audit ID
 * @returns {Promise<Object>} Report data
 */
async function getAuditReports(auditId = null) {
  const endpoint = auditId ? `reports/${auditId}` : 'reports';
  return apiGet(endpoint);
}

// Export functions for module usage (if using modules)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    apiGet,
    apiPost,
    getTransactions,
    getAnomalies,
    uploadAuditData,
    getDashboardKPIs,
    getAuditReports,
  };
}
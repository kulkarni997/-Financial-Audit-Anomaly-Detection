/* ===============================
   API.js - Central API Handler
================================ */

const API_BASE = "/api/";
const TOKEN_KEY = "access_token";
const REFRESH_KEY = "refresh_token";

/* 🔹 Core Request Function */
async function request(method, endpoint, body = null, isForm = false) {
  const headers = {};

  // Add token
  const token = localStorage.getItem(TOKEN_KEY);
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  // JSON vs FormData
  if (!isForm) {
    headers["Content-Type"] = "application/json";
  }

  try {
    let res = await fetch(API_BASE + endpoint, {
      method,
      headers,
      body: body ? (isForm ? body : JSON.stringify(body)) : null,
    });

    // 🔄 Handle expired token
    if (res.status === 401) {
      const refreshed = await refreshToken();
      if (refreshed) {
        return request(method, endpoint, body, isForm);
      } else {
        logout();
        return;
      }
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || "API error");
    }

    return await res.json();

  } catch (err) {
    console.error("API Error:", err);
    throw err;
  }
}

/* 🔹 Token Refresh */
async function refreshToken() {
  const refresh = localStorage.getItem(REFRESH_KEY);
  if (!refresh) return false;

  try {
    const res = await fetch(API_BASE + "auth/refresh/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh }),
    });

    if (!res.ok) return false;

    const data = await res.json();
    localStorage.setItem(TOKEN_KEY, data.access);
    return true;

  } catch {
    return false;
  }
}

/* 🔹 Logout */
function logout() {
  localStorage.clear();
  window.location.href = "/login/";
}

/* ===============================
   Public API Methods
================================ */

const api = {
  get: (url) => request("GET", url),
  post: (url, body) => request("POST", url, body),
  patch: (url, body) => request("PATCH", url, body),

  // Upload (FormData)
  upload: (url, formData) => request("POST", url, formData, true),
};

/* ===============================
   Specific API Calls
================================ */

// Dashboard
async function getDashboardSummary() {
  return api.get("dashboard/summary/");
}

async function getDashboardTrends() {
  return api.get("dashboard/trends/");
}

// Anomalies
async function getAnomalies(params = "") {
  return api.get(`anomalies/${params}`);
}

// Upload
async function uploadAudit(file) {
  const formData = new FormData();
  formData.append("file", file);
  return api.upload("audits/upload/", formData);
}

// Reports
async function generateReport(data) {
  return api.post("reports/generate/", data);
}
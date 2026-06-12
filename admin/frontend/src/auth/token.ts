// The admin token lives in localStorage. A tiny event lets React re-render when
// it changes (sign in / sign out).

const KEY = "duratiq_admin_token";
const EVENT = "duratiq-token-change";

export function getToken(): string {
  return localStorage.getItem(KEY) ?? "";
}

export function setToken(token: string): void {
  localStorage.setItem(KEY, token);
  window.dispatchEvent(new Event(EVENT));
}

export function clearToken(): void {
  localStorage.removeItem(KEY);
  window.dispatchEvent(new Event(EVENT));
}

export function onTokenChange(handler: () => void): () => void {
  window.addEventListener(EVENT, handler);
  return () => window.removeEventListener(EVENT, handler);
}

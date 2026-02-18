import React, { useEffect, useMemo, useState } from 'react';
import Keycloak from 'keycloak-js';

const API_BASE = process.env.REACT_APP_REPORTS_API_URL || 'http://localhost:8000';

const KC_URL = process.env.REACT_APP_KEYCLOAK_URL || 'http://localhost:8080';
const KC_REALM = process.env.REACT_APP_KEYCLOAK_REALM || 'reports-realm';
const KC_CLIENT = process.env.REACT_APP_KEYCLOAK_CLIENT || 'reports-frontend';

type ReportRow = {
  day: string;
  prosthesis_id: string;
  steps_sum: number;
  active_minutes: number;
  avg_load?: number | null;
  max_load?: number | null;
  errors_count: number;
  battery_min?: number | null;
  battery_avg?: number | null;
};

type ReportResponse = {
  user_id: string;
  from_date: string;
  to_date: string;
  available_to?: string | null;
  rows: ReportRow[];
};

function formatDate(d: Date): string {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

const keycloak = new Keycloak({
  url: KC_URL,
  realm: KC_REALM,
  clientId: KC_CLIENT,
});

const ReportPage: React.FC = () => {
  const defaultFrom = useMemo(() => {
    const d = new Date();
    d.setDate(d.getDate() - 7);
    return formatDate(d);
  }, []);
  const defaultTo = useMemo(() => formatDate(new Date()), []);

  const [from, setFrom] = useState(defaultFrom);
  const [to, setTo] = useState(defaultTo);

  const [kcReady, setKcReady] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<ReportResponse | null>(null);

  useEffect(() => {
    // init with PKCE (у тебя включено S256)
    keycloak
      .init({
        onLoad: 'check-sso',
        pkceMethod: 'S256',
        checkLoginIframe: false,
      })
      .then((auth) => {
        setAuthenticated(!!auth);
        setKcReady(true);
      })
      .catch((e) => {
        console.error('Keycloak init error', e);
        setError('Keycloak init error');
        setKcReady(true);
      });
  }, []);

  const login = () => keycloak.login();
  const logout = () => keycloak.logout({ redirectUri: window.location.origin });

  const getAccessToken = async (): Promise<string> => {
    // токен живёт 120 сек у тебя — обновляем заранее
    const refreshed = await keycloak.updateToken(30).catch(() => false);
    if (!keycloak.token) throw new Error('NO_TOKEN');
    return keycloak.token;
  };

  const getReport = async () => {
    setLoading(true);
    setError(null);
    setReport(null);

    try {
      if (!authenticated) {
        await login();
        return;
      }

      const token = await getAccessToken();

      const url = new URL('/reports', API_BASE);
      url.searchParams.set('from', from);
      url.searchParams.set('to', to);

      const resp = await fetch(url.toString(), {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: 'application/json',
        },
      });

      if (resp.status === 401) {
        // токен истёк/невалидный — пробуем перелогин
        setAuthenticated(false);
        setError('Сессия истекла. Войдите снова.');
        return;
      }

      if (resp.status === 409) {
        const body = await resp.json().catch(() => null);
        const availableTo = body?.available_to || body?.detail?.available_to;
        setError(
          availableTo
            ? `Данные ещё не готовы. Доступно до: ${availableTo}`
            : 'Данные за выбранный период ещё не готовы.'
        );
        return;
      }

      if (!resp.ok) {
        const text = await resp.text().catch(() => '');
        throw new Error(`HTTP ${resp.status}: ${text}`);
      }

      const data = (await resp.json()) as ReportResponse;
      setReport(data);
    } catch (e: any) {
      if (e?.message === 'NO_TOKEN') {
        setError('Нет access token. Войдите снова.');
      } else {
        setError(e?.message || String(e));
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 900, margin: '0 auto', padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Reports</h2>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'end' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <label>From</label>
          <input type="date" value={from} onChange={(e) => setFrom(e.target.value)} />
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <label>To</label>
          <input type="date" value={to} onChange={(e) => setTo(e.target.value)} />
        </div>

        <button onClick={authenticated ? logout : login} disabled={!kcReady || loading}>
          {authenticated ? 'Logout' : 'Login'}
        </button>

        <button onClick={getReport} disabled={!kcReady || loading || !from || !to}>
          {loading ? 'Loading…' : 'Get report'}
        </button>
      </div>

      {error && (
        <div style={{ marginTop: 16, padding: 12, background: '#ffe6e6', borderRadius: 8 }}>
          {error}
        </div>
      )}

      {report && (
        <div style={{ marginTop: 16 }}>
          <div style={{ marginBottom: 10 }}>
            <b>User:</b> {report.user_id} &nbsp;|&nbsp;
            <b>Period:</b> {report.from_date} → {report.to_date}
            {report.available_to && (
              <>
                &nbsp;|&nbsp;<b>Available to:</b> {report.available_to}
              </>
            )}
          </div>

          {report.rows.length === 0 ? (
            <div style={{ padding: 12, background: '#f3f3f3', borderRadius: 8 }}>
              Нет данных за выбранный период.
            </div>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr>
                    {['day','prosthesis_id','steps_sum','active_minutes','avg_load','max_load','errors_count','battery_avg'].map((h) => (
                      <th key={h} style={{ textAlign: 'left', padding: 8, borderBottom: '1px solid #ddd' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {report.rows.map((r, idx) => (
                    <tr key={`${r.day}-${r.prosthesis_id}-${idx}`}>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.day}</td>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.prosthesis_id}</td>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.steps_sum}</td>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.active_minutes}</td>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.avg_load ?? ''}</td>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.max_load ?? ''}</td>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.errors_count}</td>
                      <td style={{ padding: 8, borderBottom: '1px solid #eee' }}>{r.battery_avg ?? ''}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default ReportPage;
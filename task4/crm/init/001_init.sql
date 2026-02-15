-- crm/init/001_init.sql

-- Таблица CRM clients (пример)
CREATE TABLE IF NOT EXISTS public.clients (
  id              TEXT PRIMARY KEY,
  user_id         TEXT NOT NULL,
  email           TEXT NOT NULL,
  full_name       TEXT NOT NULL,
  prosthesis_id   TEXT NOT NULL,
  model           TEXT NOT NULL,
  region          TEXT NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_clients_user_prosthesis
  ON public.clients(user_id, prosthesis_id);

-- Демоданные
INSERT INTO public.clients (id, user_id, email, full_name, prosthesis_id, model, region)
VALUES
  ('c1', 'u1', 'u1@example.com', 'User One', 'p1', 'Bionic-X', 'EU'),
  ('c2', 'u2', 'u2@example.com', 'User Two', 'p2', 'Bionic-X', 'EU')
ON CONFLICT (id) DO NOTHING;
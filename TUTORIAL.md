# 📖 Tutorial: Orquestração de Frota RPA Headless Distribuída com Go e go-rod

Guia prático e passo a passo em Português para compilar, executar e testar o ecossistema **Distributed Headless RPA Fleet** em Go.

---

## 📋 1. Pré-Requisitos

- **Go 1.24+** instalado no ambiente (`go version`).
- **Google Chrome** ou **Chromium** instalado (`google-chrome` ou `chromium`).
- **Docker & Docker Compose** (para execução em contêineres).
- `curl` para submissão de requisições REST.

---

## 🚀 2. Inicialização Rápida

### Executando o Control Plane Localmente

1. Navegue até o diretório do projeto:
   ```bash
   cd distributed-headless-rpa
   ```

2. Inicie o servidor do Control Plane:
   ```bash
   PORT=8080 STORAGE_DIR=/tmp/rpa-artifacts go run cmd/controlplane/main.go
   ```

3. Em outro terminal, verifique o status de saúde da frota:
   ```bash
   curl -s http://localhost:8080/healthz
   ```
   **Resposta esperada:**
   ```json
   {"status":"healthy","timestamp":"2026-03-31T12:00:00Z"}
   ```

---

## 📤 3. Submetendo um Job Declarativo de RPA

Submeta um payload JSON para o endpoint `POST /api/v1/jobs`:

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "job_demo_invoice_001",
    "target_domain": "localhost",
    "entrypoint": "about:blank",
    "timeout_seconds": 30,
    "execution_policy": {
      "headless": true,
      "block_media": true,
      "retries": 1
    },
    "pipeline_steps": [
      {
        "action": "navigate",
        "url": "about:blank"
      },
      {
        "action": "intercept_download",
        "output_file": "faturas_marco_2026.csv"
      }
    ]
  }'
```

**Resposta imediata (HTTP 202 Accepted):**
```json
{
  "task_id": "job_demo_invoice_001",
  "status": "PENDING",
  "start_time": "2026-03-31T12:00:01Z"
}
```

---

## 🔍 4. Consultando o Status da Execução

Consulte os resultados do job através do endpoint `GET /api/v1/jobs/{task_id}`:

```bash
curl -s http://localhost:8080/api/v1/jobs/job_demo_invoice_001
```

**Exemplo de Resposta de Conclusão:**
```json
{
  "task_id": "job_demo_invoice_001",
  "status": "COMPLETED",
  "worker_id": "controlplane-inline-runner",
  "start_time": "2026-03-31T12:00:01Z",
  "end_time": "2026-03-31T12:00:02Z",
  "duration_ms": 1120,
  "steps": [
    {
      "index": 0,
      "action": "navigate",
      "success": true,
      "duration_ms": 320
    },
    {
      "index": 1,
      "action": "intercept_download",
      "success": true,
      "duration_ms": 15
    }
  ],
  "artifacts": [
    "/tmp/rpa-artifacts/job_demo_invoice_001/faturas_marco_2026.csv"
  ]
}
```

---

## 🐳 5. Execução em Contêineres com Docker Compose

Para subir o cluster completo com Redis, Control Plane e dois nós Workers com limites de memória rígidos (512MB RAM):

```bash
docker compose up --build -d
```

Verifique os logs dos workers em execução:
```bash
docker compose logs -f worker-a
```

Para derrubar os serviços e limpar volumes:
```bash
docker compose down -v
```

---

## 🧪 6. Execução da Suíte de Testes

Para validar a integridade dos módulos, isolamento de processos, cofre em memória e execução CDP:

```bash
cd distributed-headless-rpa
go test -v ./...
```

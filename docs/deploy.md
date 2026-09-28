# Deploying EoneShop to Cloud Run

Target: project `eonelabs-portfolio`, region `asia-southeast1`, public at
https://micro.eonelabs.my. Design and trade-offs: [ADR 0006](adr/0006-cloud-run.md).

```
internet ─> shop-gateway (public, micro.eonelabs.my)
              │ ID token
              ├─> shop-catalog ┐
              ├─> shop-order   ├─ private (IAM) ── Neon: one database each
              └─> shop-notification
                   shop-payment ┘
services ─publish─> Pub/Sub topics eoneshop.* ─push (OIDC)─> services
                                  └─ 5 failed attempts ─> eoneshop.dead-letter
spans ─> Cloud Trace            logs (JSON, trace-linked) ─> Cloud Logging
```

## First-time setup (in order)

Steps marked **owner** are done by hand because they involve credentials
or accounts. Every `gcloud` step changes the project and needs approval first.

1. **Base infra**: `scripts/gcp_setup.sh base`. Enables Pub/Sub + Cloud Trace,
   creates 6 service accounts and 10 topics, and grants publish rights.
   Run it again after step 2 so the secret grants succeed.

2. **owner: Neon.** In the Neon console, create a project `eoneshop`
   (region: AWS Singapore, closest to asia-southeast1) with four databases:
   `catalog`, `order`, `payment`, `notification`. Copy each **pooled**
   connection string (host contains `-pooler`), then store them. The
   values never pass through chat or the repo:

   ```bash
   for s in CATALOG ORDER PAYMENT NOTIFICATION; do
     read -rsp "$s connection string: " v; echo
     printf '%s' "$v" | gcloud secrets create "EONESHOP_${s}_DATABASE_URL" \
       --data-file=- --replication-policy=automatic --project=eonelabs-portfolio
   done
   ```

   Tables are created by each service on startup.

3. **owner: connect the repo to Cloud Build.** Console, then Cloud Build, then
   Repositories (2nd gen, region asia-southeast1). Link
   `yizhiwan/eoneshop-microservices` through the existing GitHub connection.
   This is a GitHub App permission, so only you can grant it.

4. **Trigger**: create `eoneshop-trigger` on branch `^main$` using
   `cloudbuild.yaml`, same service account as `eonelabs-trigger`. Then run it
   once (or `gcloud builds submit --config=cloudbuild.yaml`) for the first deploy.

5. **Wire events**: `scripts/gcp_setup.sh wire`. Creates 9 push subscriptions
   (with dead-lettering) and the invoker grants. It needs the service URLs,
   so it runs after the first deploy.

6. **Domain**: `scripts/gcp_setup.sh domain`, then **owner** adds
   `CNAME micro -> ghs.googlehosted.com.` at Exabytes. The certificate usually
   arrives within about an hour.

## After that

Merging to `main` deploys only the services whose code (or `shared/`) changed.
No manual deploys.

## Checks

```bash
curl https://micro.eonelabs.my/health
curl https://micro.eonelabs.my/api/products
curl -X POST https://micro.eonelabs.my/api/orders -H 'content-type: application/json' -d '{"product_id":1,"qty":1}'
curl https://micro.eonelabs.my/api/orders/1     # PENDING -> COMPLETED
gcloud pubsub subscriptions pull eoneshop.dead-letter.inspect --auto-ack --limit=5 --project=eonelabs-portfolio
```

Traces: Cloud Console, then Trace explorer, then filter on `order.ref`.

## Rollback

`gcloud run services update-traffic shop-<svc> --to-revisions=<old>=100 --region=asia-southeast1`
(gated). Old revisions stay available. No git revert needed.

## Cost

Everything scales to zero. Cloud Run, Pub/Sub, Cloud Trace and Neon all stay
within their free tiers at demo traffic. There's no scheduler, on purpose, so
Neon can suspend (ADR 0006).

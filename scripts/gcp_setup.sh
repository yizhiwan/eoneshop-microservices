#!/usr/bin/env bash
# One-time GCP setup for EoneShop (ADR 0006). Everything here changes the
# eonelabs-portfolio project, so every stage goes through the owner's approval.
#
#   scripts/gcp_setup.sh base      # APIs, service accounts, topics, IAM
#   (owner: Neon project + 4 secrets, see docs/deploy.md)
#   (first deploy: Cloud Build trigger or `gcloud builds submit`)
#   scripts/gcp_setup.sh wire      # push subscriptions + invoker grants (needs deployed services)
#   scripts/gcp_setup.sh domain    # micro.eonelabs.my -> shop-gateway
#
# Safe to re-run: "already exists" errors are ignored.
set -uo pipefail

PROJECT=eonelabs-portfolio
REGION=asia-southeast1
P="--project=$PROJECT"
PREFIX=eoneshop.
SERVICES=(gateway catalog order payment notification feed)
DB_SERVICES=(catalog order payment notification)

sa() { echo "shop-$1-run@$PROJECT.iam.gserviceaccount.com"; }
PUSH_SA="shop-pubsub-push@$PROJECT.iam.gserviceaccount.com"
# Run a create command; stay quiet if the thing already exists (re-runs),
# but print everything if it failed for any other reason.
ok() {
  local out; out=$("$@" 2>&1) && { echo "$out"; return; }
  grep -q "already exists" <<< "$out" || echo "$out"
}

# Which service publishes which topics (least privilege: publish only your own).
declare -A PUBLISHES=(
  [order]="order.created order.completed order.cancelled"
  [catalog]="stock.reserved stock.rejected stock.released"
  [payment]="payment.succeeded payment.failed payment.refunded"
)
# topic -> subscribing service
SUBSCRIPTIONS=(
  "order.created catalog"
  "stock.reserved payment"
  "stock.rejected order"
  "payment.succeeded order"
  "payment.failed order"
  "order.completed notification"
  "order.cancelled notification"
  "order.cancelled catalog"
  "order.cancelled payment"
)
# The visualizer's read-only tap (ADR 0007): every topic, plus dead letters.
TAP_TOPICS=(order.created stock.reserved stock.rejected stock.released payment.succeeded
            payment.failed payment.refunded order.completed order.cancelled dead-letter)

base() {
  gcloud services enable pubsub.googleapis.com cloudtrace.googleapis.com $P

  for s in "${SERVICES[@]}"; do
    ok gcloud iam service-accounts create "shop-$s-run" --display-name="eoneshop-$s-cloud-run" $P
    # Everyone writes traces; logs need no grant on Cloud Run.
    gcloud projects add-iam-policy-binding $PROJECT --member="serviceAccount:$(sa $s)" \
      --role=roles/cloudtrace.agent --condition=None --quiet >/dev/null
  done
  ok gcloud iam service-accounts create shop-pubsub-push --display-name="eoneshop-pubsub-push-identity" $P

  for t in order.created order.completed order.cancelled stock.reserved stock.rejected stock.released \
           payment.succeeded payment.failed payment.refunded dead-letter; do
    ok gcloud pubsub topics create "$PREFIX$t" $P
  done
  ok gcloud pubsub subscriptions create "${PREFIX}dead-letter.inspect" --topic="${PREFIX}dead-letter" $P

  for s in "${!PUBLISHES[@]}"; do
    for t in ${PUBLISHES[$s]}; do
      gcloud pubsub topics add-iam-policy-binding "$PREFIX$t" --member="serviceAccount:$(sa $s)" \
        --role=roles/pubsub.publisher $P >/dev/null
    done
  done

  # Each database service can read only its own DATABASE_URL secret.
  # (The secrets are created by the owner first; see docs/deploy.md.)
  for s in "${DB_SERVICES[@]}"; do
    secret="EONESHOP_$(echo "$s" | tr a-z A-Z)_DATABASE_URL"
    gcloud secrets add-iam-policy-binding "$secret" --member="serviceAccount:$(sa $s)" \
      --role=roles/secretmanager.secretAccessor $P >/dev/null \
      || echo "!! secret $secret missing: create it first (docs/deploy.md)"
  done
  echo "base: done"
}

wire() {
  number=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
  pubsub_agent="service-$number@gcp-sa-pubsub.iam.gserviceaccount.com"
  url() { gcloud run services describe "shop-$1" --region=$REGION --format='value(status.url)' $P; }
  invoker() {  # invoker <service> <member>
    gcloud run services add-iam-policy-binding "shop-$1" --region=$REGION \
      --member="$2" --role=roles/run.invoker $P >/dev/null
  }

  # Pub/Sub signs push requests as shop-pubsub-push; Cloud Run checks the token.
  gcloud iam service-accounts add-iam-policy-binding "$PUSH_SA" \
    --member="serviceAccount:$pubsub_agent" --role=roles/iam.serviceAccountTokenCreator $P >/dev/null
  # Dead-lettering: Pub/Sub itself must publish to the DLQ topic.
  gcloud pubsub topics add-iam-policy-binding "${PREFIX}dead-letter" \
    --member="serviceAccount:$pubsub_agent" --role=roles/pubsub.publisher $P >/dev/null

  for s in catalog order payment notification feed; do
    invoker "$s" "serviceAccount:$PUSH_SA"
  done
  # The gateway calls these directly (ID token audience = service URL).
  for s in catalog order notification feed; do
    invoker "$s" "serviceAccount:$(sa gateway)"
  done

  for entry in "${SUBSCRIPTIONS[@]}"; do
    read -r topic svc <<< "$entry"
    name="$PREFIX$topic--$svc"
    ok gcloud pubsub subscriptions create "$name" --topic="$PREFIX$topic" \
      --push-endpoint="$(url "$svc")/pubsub/push" \
      --push-auth-service-account="$PUSH_SA" \
      --ack-deadline=30 --min-retry-delay=1s --max-retry-delay=60s \
      --dead-letter-topic="${PREFIX}dead-letter" --max-delivery-attempts=5 $P
    gcloud pubsub subscriptions add-iam-policy-binding "$name" \
      --member="serviceAccount:$pubsub_agent" --role=roles/pubsub.subscriber $P >/dev/null
  done
  # Tap subscriptions: no dead-lettering (a tap failure isn't an order
  # failure) and short retention, so a sleeping feed never builds a backlog.
  for topic in "${TAP_TOPICS[@]}"; do
    ok gcloud pubsub subscriptions create "$PREFIX$topic--feed" --topic="$PREFIX$topic" \
      --push-endpoint="$(url feed)/pubsub/push" \
      --push-auth-service-account="$PUSH_SA" \
      --ack-deadline=10 --message-retention-duration=10m $P
  done
  echo "wire: done"
}

domain() {
  ok gcloud beta run domain-mappings create --service=shop-gateway \
    --domain=micro.eonelabs.my --region=$REGION $P
  echo "domain: owner adds CNAME  micro -> ghs.googlehosted.com.  at Exabytes"
}

case "${1:-}" in
  base) base ;;
  wire) wire ;;
  domain) domain ;;
  *) echo "usage: $0 base|wire|domain"; exit 2 ;;
esac

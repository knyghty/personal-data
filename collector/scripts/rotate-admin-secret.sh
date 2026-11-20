#!/bin/sh
# Rotate ADMIN_SECRET: set it on the Fly app, recreate the hourly trigger machine with it,
# and store it as the GitHub Actions secret the deploy workflow uses for its smoke run.
# Run from anywhere with flyctl and gh logged in. The secret is never printed.
set -eu

APP=glimmering-voice-610
REPO=knyghty/personal-data
URL=https://$APP.fly.dev

secret=$(openssl rand -hex 32)

fly secrets set ADMIN_SECRET="$secret" -a "$APP"

for id in $(fly machine list -a "$APP" --json | jq -r '.[] | select(.name == "hourly-trigger") | .id'); do
  fly machine destroy "$id" -a "$APP" --force
done
fly machine run curlimages/curl -a "$APP" --name hourly-trigger --schedule hourly \
  --vm-memory 256 --restart no \
  -- curl -fsS --max-time 3000 -X POST -H "Authorization: Bearer $secret" "$URL/admin/run"

printf '%s' "$secret" | gh secret set ADMIN_SECRET -R "$REPO"
echo "rotated"

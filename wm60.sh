#!/bin/bash

ADMINPWD='No1WantsRulers'
IP=192.168.86.49

TOKEN_JSON=$(printf '{"cmd":"get_token"}\n' | nc $IP 4028)

TIME=$(python3 - <<'PY' <<<"$TOKEN_JSON"
import json,sys; print(json.load(sys.stdin)["Msg"]["time"])
PY
)
SALT=$(python3 - <<'PY' <<<"$TOKEN_JSON"
import json,sys; print(json.load(sys.stdin)["Msg"]["salt"])
PY
)
NEWSALT=$(python3 - <<'PY' <<<"$TOKEN_JSON"
import json,sys; print(json.load(sys.stdin)["Msg"]["newsalt"])
PY
)

# key = md5crypt(salt, admin_password) → take the hash field
KEY=$(openssl passwd -1 -salt "$SALT" "$ADMINPWD" | cut -d'$' -f4)

# sign = md5(newsalt + key + last 4 chars of time)
LAST4=${TIME: -4}
SIGN=$(printf "%s" "${NEWSALT}${KEY}${LAST4}" | md5 | awk '{print $2}')

# aeskey = sha256(KEY) (hex)
AESKEY_HEX=$(printf "%s" "$KEY" | openssl dgst -sha256 -binary | xxd -p -c256)

PLAINTEXT="token,${SIGN}|set_power_pct|50"
ENC=$(printf "%s" "$PLAINTEXT" | openssl enc -aes-256-ecb -K "$AESKEY_HEX" -nosalt -base64)

printf '{"enc":1,"data":"%s"}\n' "$ENC" | nc $IP 4028

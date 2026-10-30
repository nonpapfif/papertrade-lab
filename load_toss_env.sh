if [ "${ZSH_EVAL_CONTEXT##*:}" != "file" ] && [ -z "${BASH_VERSION:-}" ]; then
    printf '%s\n' 'Load this file with source in zsh or bash.' >&2
    exit 1
fi

load_toss_credentials() {
    local credential_name credential_path credential_value

    for credential_name in TOSS_CLIENT_ID TOSS_CLIENT_SECRET; do
        credential_path="${HOME}/.config/secrets/${credential_name}"
        if [ ! -f "$credential_path" ] || [ ! -r "$credential_path" ]; then
            printf 'Missing or unreadable credential file: %s\n' "$credential_path" >&2
            return 1
        fi
        credential_value="$(cat "$credential_path")" || return 1
        if [ -z "$credential_value" ]; then
            printf 'Credential file is empty: %s\n' "$credential_path" >&2
            return 1
        fi
    done

    TOSS_CLIENT_ID="$(cat "${HOME}/.config/secrets/TOSS_CLIENT_ID")" || return 1
    TOSS_CLIENT_SECRET="$(cat "${HOME}/.config/secrets/TOSS_CLIENT_SECRET")" || return 1
    export TOSS_CLIENT_ID TOSS_CLIENT_SECRET
}

load_toss_credentials

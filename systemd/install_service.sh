#!/usr/bin/env bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$HOME/.config/systemd/user/"
cp -v "$script_dir"/nuliga_event_search.{service,timer} "$HOME/.config/systemd/user/"
systemctl --user daemon-reload

systemctl --user enable nuliga_event_search.service
systemctl --user restart nuliga_event_search.service
systemctl --user status nuliga_event_search.service --no-pager -l

systemctl --user enable nuliga_event_search.timer
systemctl --user restart nuliga_event_search.timer
systemctl --user status nuliga_event_search.timer --no-pager -l

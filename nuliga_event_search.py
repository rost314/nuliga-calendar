#!/usr/bin/env python
import datetime
import difflib
import itertools
import json
import re
from zoneinfo import ZoneInfo
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd
import bs4 as bs
import requests
import icalendar

MY_TEAMS = (
    'TC Alfeld Herren 30',
    'TC Alfeld Herren 50',
    'TC Alfeld Damen 40',
    'TSV Gronau Damen 30',
    'TV Bad Münder Damen 30',
    'HTC Hannover Herren 50'
)

BASE_URL = 'https://tnb.liga.nu'

pd.set_option('max_colwidth', 0)


def find_team_links(group_url):
    liga_soup = bs.BeautifulSoup(requests.get(group_url).content, features='xml')
    tables = liga_soup.find_all('table')
    [teams_table] = [t for t in tables if t.text.strip().startswith('Rang')]
    return {a.text.strip(): f"{BASE_URL}{a['href']}" for a in teams_table.find_all('a')}


def extract_relevant_tables(team_url):
    soup = bs.BeautifulSoup(requests.get(team_url).content, features='xml')
    tables = soup.find_all('table')
    [overview_table] = [t for t in tables if t.text.strip().startswith('Verein')]
    [event_table] = [t for t in tables if t.text.strip().startswith('Datum')]
    return overview_table, event_table


def extract_event_table(table):
    column_names = ['Tag', 'Datum', 'x'] + [th.text.strip() for th in table.find_all('th')][1:] or None
    if 'Heimmannschaft' not in column_names:
        print(column_names)
        raise Exception('could not find "Heimmannschaft" in header, seems wrong table')
    data = []
    for row in table.find_all('tr'):
        row_data = []
        for td in row.find_all('td'):
            row_data.append(''.join(td.stripped_strings))
        if row_data:
            data.append(row_data)
    df = pd.DataFrame(data, columns=column_names)
    df = df.drop('x', axis='columns')
    return df


def create_event_dict(portrait, team_links, event_row):
    try:
        begin = datetime.datetime.strptime(event_row.Datum, '%d.%m.%Y %H:%M')
    except ValueError:
        begin = datetime.datetime.strptime(event_row.Datum, '%d.%m.%Y').replace(hour=0, minute=0)

    begin = begin.replace(tzinfo=ZoneInfo('Europe/Berlin')).astimezone(ZoneInfo('UTC'))
    end = begin + datetime.timedelta(hours=6)

    group = portrait["Liga"]
    home_team = event_row.Heimmannschaft
    guest_team = event_row.Gastmannschaft
    group_url = portrait["group_url"]
    home_team_url = team_links[home_team]
    guest_team_url = team_links[guest_team]

    return {
        'uid': f'{group}: {home_team} vs. {guest_team}',
        'group': group,
        'group_url': group_url,
        'home_team': home_team,
        'home_team_url': home_team_url,
        'guest_team': guest_team,
        'guest_team_url': guest_team_url,
        'start': begin.isoformat(),
        'end': end.isoformat(),
        'meeting_url': event_row.meeting_link if hasattr(event_row, 'meeting_link') else None,
    }


def calendar_dict_from_event_dict(event_dict):
    description = [
        f'{event_dict["home_team"]} vs. {event_dict["guest_team"]}',
        f'<a href={event_dict["group_url"]}>{event_dict["group"]}</a>',
        f'<a href={event_dict["home_team_url"]}>{event_dict["home_team"]}</a>',
        f'<a href={event_dict["guest_team_url"]}>{event_dict["guest_team"]}</a>',
        *([f'<a href={event_dict["meeting_url"]}>Spielbericht</a>'] if event_dict.get('meeting_url') else [])
    ]
    return {
        'uid': event_dict['uid'],
        'summary': event_dict['group'],
        'description': '<br>'.join(description),
        'dtstart': event_dict['start'],
        'dtend': event_dict['end'],
        'location': event_dict['home_team'],
        'organizer': 'TNB'
    }


def create_icalendar_event(event_dict, dtstamp):
    calendar_dict = calendar_dict_from_event_dict(event_dict)
    event = icalendar.Event()
    event.add('dtstamp', dtstamp)
    event.add('uid', calendar_dict['uid'])
    event.add('summary', calendar_dict['summary'])
    event.add('description', calendar_dict['description'])
    event.add('dtstart', datetime.datetime.fromisoformat(calendar_dict['dtstart']))
    event.add('dtend', datetime.datetime.fromisoformat(calendar_dict['dtend']))
    event.add('location', calendar_dict['location'])
    event.add('organizer', calendar_dict['organizer'])
    return event


def create_team_portrait(team_url, group_url):
    [df] = pd.read_html(team_url, match='Mannschaft', index_col=0)
    [team_portrait] = df.to_dict().values()
    team_portrait['group_url'] = group_url
    return team_portrait


def find_club_teams(season, club_url):
    soup = bs.BeautifulSoup(requests.get(club_url).content, features='xml')
    tables = soup.find_all('table')
    club_table = tables[-1]
    correct_table_section = False
    teams = []
    for row in club_table.find_all('tr'):
        if 'table-split' in row.get('class', []):
            correct_table_section = season in row.text.split('\n')
        elif correct_table_section:
            links = row.find_all('a')
            if links:
                team_url, group_url = [f'{BASE_URL}{a["href"]}' for a in links[:2]]
                team_short_name = re.sub(r' \(.*?\)', '', links[0].text)
                teams.append((team_short_name, team_url, group_url))
    return teams


def write_calender(events, ics_file, dtstamp):
    cal = icalendar.Calendar(method='REQUEST')
    for event in events:
        cal.add_component(create_icalendar_event(event, dtstamp=dtstamp))
    with open(ics_file, 'wb') as f:
        f.write(cal.to_ical())
    print(f'wrote "{ics_file}"')


def process_team_events(team_url, group_url):
    overview_table, event_table = extract_relevant_tables(team_url)
    team_portrait = create_team_portrait(team_url, group_url)
    print(team_portrait['Mannschaft'], team_portrait['Liga'], sep=': ')
    team_links = find_team_links(group_url)
    name = f'{team_portrait["Mannschaft"]} {team_portrait["Liga"]}'
    event_df = extract_event_table(event_table)
    events = [create_event_dict(team_portrait, team_links, event_row)
              for event_row in event_df.itertuples(index=False)]
    return name, {'dataframe': event_df, 'events': events}


def process_season_clubs(season, clubs, filter_my_teams=True):
    clubs_data = {}
    for club, club_url in clubs.items():
        clubs_data[club] = {}
        teams = find_club_teams(season, club_url)
        for team_short_name, team_url, group_url in teams:
            if filter_my_teams and f'{club} {team_short_name}' not in MY_TEAMS:
                print(f'Skipping {club} {team_short_name}')
                continue
            try:
                team_name, team_data = process_team_events(team_url, group_url)
                clubs_data[club][team_name] = team_data
                try:
                    display(team_data['dataframe'])
                except NameError:
                    from tabulate import tabulate
                    print(tabulate(team_data['dataframe'], headers='keys', tablefmt='psql', showindex=False))
            except Exception as error:
                print(error)
    return clubs_data


def convert_to_google_event(event_dict):
    calendar_dict = calendar_dict_from_event_dict(event_dict)
    return {
        'summary': calendar_dict['uid'],
        'description': calendar_dict['description'],
        'location': calendar_dict['location'],
        'start': {'dateTime': calendar_dict['dtstart'], 'timeZone': 'UTC'},
        'end': {'dateTime': calendar_dict['dtend'], 'timeZone': 'UTC'},
        'organizer': {'displayName': calendar_dict['organizer']},
    }


def make_user_credentials():
    token_json = Path('token.json')
    credentials_json = 'credentials.json'
    scopes = ['https://www.googleapis.com/auth/calendar']
    creds = None
    if token_json.is_file():
        creds = Credentials.from_authorized_user_file(token_json, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_json, scopes)
            creds = flow.run_local_server(port=0)
        token_json.write_text(creds.to_json(), encoding='utf-8')
    return creds


def make_service_account_credentials():
    token_json = Path('tennis-calendar-384409-3a263b4006c8.json')  # Replace with the path to your JSON key file
    scopes = ['https://www.googleapis.com/auth/calendar']
    creds = None
    if Path(token_json).is_file():
        creds = service_account.Credentials.from_service_account_file(token_json, scopes=scopes)
    if not creds or not creds.valid:
        creds.refresh(Request())
    return creds


def get_api_service():
    creds = make_service_account_credentials()
    return build('calendar', 'v3', credentials=creds)


def get_calendars():
    creds = make_user_credentials()
    service = build('calendar', 'v3', credentials=creds)
    calendars = {c['summary']: c['id'] for c in service.calendarList().list().execute()['items']}
    Path('calendars.json').write_text(json.dumps(calendars), encoding='utf-8')
    return calendars


def update_calendar_events(service, calendar_id, new_events):
    try:
        existing_events = {e['summary']: e
                           for e in service.events().list(calendarId=calendar_id).execute().get('items', [])}

        for event_dict in itertools.chain(*new_events.values()):
            google_event = convert_to_google_event(event_dict)
            existing_event_id = existing_events.get(google_event['summary'], {}).get('id')
            if existing_event_id:
                event = service.events().update(calendarId=calendar_id, eventId=existing_event_id,
                                                body=google_event).execute()
                print(f'Event updated: {event.get("htmlLink")}')
            else:
                event = service.events().insert(calendarId=calendar_id, body=google_event).execute()
                print(f'Event created: {event.get("htmlLink")}')
    except HttpError as error:
        print(f'An error occurred: {error}')


def delete_all_events(service, calendar_name, calendar_id):
    existing_events = service.events().list(calendarId=calendar_id).execute().get('items', [])
    if input(f'\nDo you really want to delete {len(existing_events)} from "{calendar_name}"? (yes/no) ') == 'yes':
        for event in existing_events:
            service.events().delete(calendarId=calendar_id, eventId=event['id']).execute()


def main():
    # uncomment to write new calendars.json calendar_id map
    # calendars = get_calendars()

    calendars = json.loads(Path('calendars.json').read_bytes())

    service = get_api_service()

    season = 'Winter 2025/2026'
    clubs = {
        'TC Alfeld': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16473',
        'TSV Gronau': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16486',
        'TV Bad Münder': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16584',
        'HTC Hannover': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16445',
    }

    clubs_data = process_season_clubs(season=season, clubs=clubs, filter_my_teams=True)
    my_teams = {
        team
        for club_data in clubs_data.values() for team in club_data
        if any(s in team for s in MY_TEAMS)
    }

    my_teams_events = {team: td['events']
                       for team, td in itertools.chain(*(team_data.items() for team_data in clubs_data.values()))
                       if team in my_teams}

    calendar_name = 'Tennis'
    calendar_id = calendars[calendar_name]
    clean = False
    if clean:
        delete_all_events(service, calendar_name=calendar_name, calendar_id=calendar_id)
    update_calendar_events(service, calendar_id, my_teams_events)

    # Create a calendar per club
    clubs = {
        # 'TC Alfeld': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16473',
        # 'TSV Gronau': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16486',
        # 'HTC Hannover': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16445',
    }
    clubs_data = process_season_clubs(season=season, clubs=clubs, filter_my_teams=False)
    for club, club_data in clubs_data.items():
        club_events = {team_name: team_data['events'] for team_name, team_data in club_data.items()}
        calendar_name = f'{club} {season}'
        calendar_id = calendars.get(calendar_name)
        if not calendar_id:
            print(f'You need to create a calendar first: "{calendar_name}"')
            continue
        update_calendar_events(service, calendar_id, club_events)


    # output_dir = Path('.')
    # for club, club_data in clubs_data.items():
    #     folder = output_dir / club
    #     folder.mkdir(parents=True, exist_ok=True)
    #     club_events = {team_name: team_data['events'] for team_name, team_data in club_data.items()}
    #     new_json = json.dumps(club_events, indent=2)
    #     json_file = output_dir / f'{season} {club}.json'.replace('/', '_')
    #     if json_file.is_file():
    #         differences = list(difflib.unified_diff(json_file.read_text(encoding='utf-8').splitlines(),
    #                                                 new_json.splitlines(),
    #                                                 fromfile=str(json_file), tofile='new', n=10))
    #         if differences:
    #             print(*differences, sep='\n')
    #             # keep a copy of the changed old file
    #             mod_time = int(Path(json_file).stat().st_mtime)
    #             json_file.with_stem(f'{json_file.stem}_{mod_time}').write_bytes(json_file.read_bytes())
    #
    #     dtstamp = datetime.datetime.now().astimezone(ZoneInfo('UTC')).replace(microsecond=0)
    #
    #     calendar_name = f'{club} {season}'
    #     calendar_id = calendars.get(calendar_name)
    #     if not calendar_id:
    #         print(f'You need to create a calendar first: "{calendar_name}"')
    #         continue
    #     clean = False
    #     if clean:
    #         delete_all_events(service, calendar_name=calendar_name, calendar_id=calendar_id)
    #
    #     force_update = False
    #     if force_update or not json_file.is_file() or differences:
    #         json_file.write_text(new_json, encoding='utf-8')
    #
    #         for team_name, team_data in club_data.items():
    #             ics_file = folder / f'{season} {team_name}.ics'.replace('/', '_')
    #             write_calender(team_data['events'], ics_file, dtstamp=dtstamp)
    #
    #         ics_file = output_dir / f'{season} {club}.ics'
    #         write_calender(itertools.chain(*club_events.values()), ics_file, dtstamp=dtstamp)
    #
    #         update_calendar_events(service, calendar_id, club_events)


if __name__ == '__main__':
    main()

#!/usr/bin/env python
from pathlib import Path


import arrow
import pandas as pd
import bs4 as bs
import requests
import icalendar

BASE_URL = 'https://tnb.liga.nu'

pd.set_option('max_colwidth', 0)


def find_team_links(group_url):
    liga_soup = bs.BeautifulSoup(requests.get(group_url).content, features='xml')
    return {a.text.strip(): f"{BASE_URL}{a['href']}" for a in liga_soup.find_all('table')[2].find_all('a')}


def extract_relevant_tables(team_url):
    soup = bs.BeautifulSoup(requests.get(team_url).content, features='xml')
    tables = soup.find_all('table')
    if len(tables) <= 3:
        # print(tables[-1])
        raise Exception('could not find expected nr. of tables > 3')
    overview_table, event_table = tables[2:4]
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


def create_cal_event(portrait, team_links, event_row):
    try:
        begin = arrow.get(event_row.Datum, 'DD.MM.YYYY HH:mm', tzinfo='Europe/Berlin')
    except arrow.parser.ParserError:
        begin = arrow.get(event_row.Datum, 'DD.MM.YYYY', tzinfo='Europe/Berlin').replace(hour=10, minute=1)

    league_name = portrait["Liga"]
    event = icalendar.Event()
    event.add('dtstart', begin.datetime)
    event.add('dtstamp', arrow.now().datetime)
    event.add('uid', f'{league_name}: {event_row.Heimmannschaft} vs. {event_row.Gastmannschaft}')
    event.add('summary', league_name)
    [gegnermannschaft] = {event_row.Heimmannschaft, event_row.Gastmannschaft} - {portrait['Mannschaft']}
    description = [f'{event_row.Heimmannschaft} vs. {event_row.Gastmannschaft}',
                   f'<a href={portrait["group_url"]}>{league_name}</a>',
                   f'<a href={team_links[gegnermannschaft]}>{gegnermannschaft}</a>']
    if hasattr(event_row, 'meeting_link'):
        description.append(f'<a href={event_row.meeting_link}>Spielbericht</a>')
    event.add('description', '<br>'.join(description))
    event.add('dtstart', begin.datetime)
    event.add('dtend', begin.shift(hours=6).datetime)
    event.add('location', event_row.Heimmannschaft)
    event.add('organizer', 'TNB')
    return event


def create_team_portrait(team_url, group_url):
    [df] = pd.read_html(team_url, match='Mannschaft', parse_dates=True, index_col=0)
    [team_portrait] = df.to_dict().values()
    team_portrait['group_url'] = group_url
    return team_portrait


def create_calender(event_df, team_portrait, team_links):
    cal = icalendar.Calendar(method='REQUEST')
    for event_row in event_df.itertuples(index=False):
        event = create_cal_event(team_portrait, team_links, event_row)
        cal.add_component(event)
    return cal


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
                teams.append((team_url, group_url))
    return teams


def write_calender(cal, ics_file):
    with open(ics_file, 'wb') as f:
        f.write(cal.to_ical())
    print(f'wrote "{ics_file}"')


def process_team_events(team_url, group_url):
    overview_table, event_table = extract_relevant_tables(team_url)
    event_df = extract_event_table(event_table)
    team_portrait = create_team_portrait(team_url, group_url)
    print(team_portrait['Mannschaft'], team_portrait['Liga'], sep=': ')
    team_links = find_team_links(group_url)
    calendar = create_calender(event_df, team_portrait, team_links)
    name = f'{team_portrait["Mannschaft"]} {team_portrait["Liga"]}'
    return name, {'dataframe': event_df, 'calendar': calendar}


def process_season_clubs(season, clubs, output_dir: Path):
    for club, club_url in clubs.items():
        teams = find_club_teams(season, club_url)
        for team_url, group_url in teams:
            try:
                team_name, team_data = process_team_events(team_url, group_url)
                try:
                    display(team_data['dataframe'])
                except NameError:
                    from tabulate import tabulate
                    print(tabulate(team_data['dataframe'], headers='keys', tablefmt='psql', showindex=False))
                folder = output_dir / club
                folder.mkdir(parents=True, exist_ok=True)
                ics_file = folder / f'{season} {team_name}.ics'
                write_calender(team_data['calendar'], ics_file)
            except Exception as error:
                print(error)
        break


if __name__ == '__main__':
    SEASON = 'Sommer 2023'
    CLUBS = {
        'TC Alfeld Mannschaften': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16473',
        'TSV Gronau Mannschaften': 'https://tnb.liga.nu/cgi-bin/WebObjects/nuLigaTENDE.woa/wa/clubTeams?club=16486',
    }

    process_season_clubs(season=SEASON, clubs=CLUBS, output_dir=Path('.'))




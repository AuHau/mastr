import csv
import logging
import math

import click
from zeep import Transport, Settings, Client
from zeep.cache import SqliteCache
from zeep.exceptions import Fault
from zeep.helpers import serialize_object

## Settings
field_names = ('EinheitMastrNummer', 'Name', 'Einheitart', 'Einheittyp', 'Standort', 'Bruttoleistung',
               'Erzeugungsleistung', 'EinheitBetriebsstatus', 'Anlagenbetreiber', 'EegMastrNummer', 'KwkMastrNummer',
               'SpeMastrNummer', 'GenMastrNummer', 'BestandsanlageMastrNummer', 'NichtVorhandenInMigriertenEinheiten')
##################


@click.command()
@click.option('-a', '--api-key', envvar='API_KEY')
@click.option('-m', '--mastr-number', envvar='MASTR_NUMBER')
@click.option('-l', '--limit', type=click.INT, default=10000)
@click.option('-s', '--step', type=click.INT, default=2000)
@click.option('-t', '--start', type=click.INT, default=0)
@click.option('-f', '--filter-type', default='Solareinheit')
@click.option('-o', '--output', 'units_file', type=click.File('w'), default='data/units.csv')
def main(api_key, mastr_number, limit, step, start, filter_type, units_file):
    wsdl = 'https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl'
    transport = Transport(cache=SqliteCache())
    settings = Settings(strict=False, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=transport, settings=settings)
    client_bind = client.bind('Marktstammdatenregister','Anlage')

    writer = csv.DictWriter(units_file, field_names)
    writer.writeheader()
    total = total_filtered = 0

    logging.getLogger('zeep').setLevel(logging.CRITICAL)

    for current_step in range(start, math.ceil(limit/step)):
        try:
            try:
                c = client_bind.GetListeAlleEinheiten(apiKey=api_key, marktakteurMastrNummer=mastr_number,
                                                      startAb=step*current_step, limit=step)
                respond = serialize_object(c)
            except Fault as e:
                print('Probably reached the end, saving file: ' + e.message)
                break

            filtered = [dict(x) for x in respond['Einheiten'] if x['Einheittyp'] == filter_type]

            print(f'[{current_step}] Fetched {len(respond["Einheiten"])} entries out of which {len(filtered)} was used')
            writer.writerows(filtered)
            total += len(respond["Einheiten"])
            total_filtered += len(filtered)
        except KeyboardInterrupt:
            print('Aborting fetching!')
            break

    print(f'Fetched in total {total} entries which {total_filtered} was used.')


if __name__ == '__main__':
    main()

import csv
import logging
import multiprocessing
import pathlib
import signal
import sys

import click
import enlighten
from zeep import Transport, Settings, Client
from zeep.cache import InMemoryCache
from zeep.exceptions import Fault
from zeep.helpers import serialize_object
import backoff

## Settings
QUEUE_SIZE = 20
BATCH_CHUNK_SIZE = 100
ERRORS_LIMIT = 20
TERMINATION_SENTINEL = object()

field_names = ['Ergebniscode', 'AufrufVeraltet', 'AufrufLebenszeitEnde', 'AufrufVersion', 'EinheitMastrNummer',
               'DatumLetzteAktualisierung', 'LokationMastrNummer', 'NetzbetreiberpruefungStatus',
               'NetzbetreiberpruefungDatum', 'AnlagenbetreiberMastrNummer', 'Land', 'Bundesland', 'Landkreis',
               'Gemeinde', 'Gemeindeschluessel', 'Postleitzahl', 'Gemarkung', 'FlurFlurstuecknummern', 'Strasse',
               'StrasseNichtGefunden', 'Hausnummer', 'HausnummerNichtGefunden', 'Adresszusatz', 'Ort', 'Laengengrad',
               'Breitengrad', 'UtmZonenwert', 'UtmEast', 'UtmNorth', 'GaussKruegerHoch', 'GaussKruegerRechts',
               'Meldedatum', 'GeplantesInbetriebnahmedatum', 'Inbetriebnahmedatum', 'DatumEndgueltigeStilllegung',
               'DatumBeginnVoruebergehendeStilllegung', 'DatumWiederaufnahmeBetrieb', 'EinheitBetriebsstatus',
               'BestandsanlageMastrNummer', 'NichtVorhandenInMigriertenEinheiten', 'NameStromerzeugungseinheit', 'Weic',
               'WeicDisplayName', 'Kraftwerksnummer', 'Energietraeger', 'Bruttoleistung', 'Nettonennleistung',
               'AnschlussAnHoechstOderHochSpannung', 'Schwarzstartfaehigkeit', 'Inselbetriebsfaehigkeit',
               'Einsatzverantwortlicher', 'FernsteuerbarkeitNb', 'FernsteuerbarkeitDv', 'FernsteuerbarkeitDr',
               'Einspeisungsart', 'PraequalifiziertFuerRegelenergie', 'GenMastrNummer',
               'zugeordneteWirkleistungWechselrichter', 'GemeinsamerWechselrichterMitSpeicher', 'AnzahlModule', 'Lage',
               'Leistungsbegrenzung', 'EinheitlicheAusrichtungUndNeigungswinkel', 'Hauptausrichtung',
               'HauptausrichtungNeigungswinkel', 'Nebenausrichtung', 'NebenausrichtungNeigungswinkel',
               'InAnspruchGenommeneFlaeche', 'ArtDerFlaeche', 'InAnspruchGenommeneAckerflaeche', 'Nutzungsbereich',
               'EegMastrNummer']

logger = logging.getLogger('fetcher')
back_logger = logging.getLogger('backoff')


##################

@backoff.on_exception(backoff.expo, Fault, max_tries=3, logger=back_logger)
def fetch_unit(client_bind, api_key, mastr_number, unit_number):
    return client_bind.GetEinheitSolar(apiKey=api_key, marktakteurMastrNummer=mastr_number,
                                       einheitMastrNummer=unit_number)


def process_units(queue: multiprocessing.Queue, process_no, api_key, mastr_number, output):
    logging.getLogger('zeep').setLevel(logging.CRITICAL)
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    force_termination = False
    def terminate(*args):
        nonlocal force_termination

        if force_termination:
            logger.warning(f'Process {process_no}: Force termination')
            raise KeyboardInterrupt('Force exit')
        else:
            logger.info(f'Process {process_no}: Set to gracefully terminate')
            force_termination = True
    signal.signal(signal.SIGINT, terminate)

    wsdl = 'https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl'
    transport = Transport(cache=InMemoryCache())
    settings = Settings(strict=False, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=transport, settings=settings)
    client_bind = client.bind('Marktstammdatenregister', 'Anlage')

    with output.open('w') as f:
        writer = csv.DictWriter(f, field_names)
        writer.writeheader()

        while True:
            unit_mastr_numbers = queue.get(block=True)
            logger.info(f'Process {process_no}: Processing next batch')

            if unit_mastr_numbers == TERMINATION_SENTINEL:
                logger.info(f'Process {process_no}: Received termination sentinel -> no more data to process.')
                break

            errors_count = 0
            for unit_number in unit_mastr_numbers:
                if force_termination:
                    logger.info(f'Process {process_no}: Gracefully terminating')
                    return

                if errors_count > ERRORS_LIMIT:
                    logger.warning(f'Process {process_no}: Reached errors limit, discarding this batch')
                    break

                try:
                    c = fetch_unit(client_bind, api_key, mastr_number, unit_number)
                    respond = serialize_object(c)
                    writer.writerow({k: (v.get('Wert', '<<unknown structure>>') if hasattr(v, 'get') else v) for k, v in respond.items()})

                    # We got successful reply, the previous errors might not be related ==> continue
                    errors_count = 0
                except Fault as e:
                    logger.warning(f'Process {process_no}: Got error, but continuing: {e.message}')
                    errors_count += 1


def process_file(input_file: pathlib.Path, api_key, mastr_number, index, output, parallelization):
    queue = multiprocessing.Queue(maxsize=QUEUE_SIZE)

    logger.info(f'Setting up process pool with {parallelization} processes')
    process_pool = []
    manager = enlighten.get_manager()

    for i in range(parallelization):
        output_file = output / input_file.name.replace('.csv', f'.out-{i}.csv')
        proc = multiprocessing.Process(target=process_units, args=(queue, i, api_key, mastr_number, output_file))
        proc.start()
        process_pool.append(proc)

    with input_file.open('r', newline='') as f:
        reader = csv.reader(f)
        finish = False

        while True:
            try:
                batch = []
                for _ in range(BATCH_CHUNK_SIZE):
                    try:
                        batch.append(next(reader)[index])
                    except StopIteration:
                        finish = True
                        break

                queue.put(batch, block=True)
            except KeyboardInterrupt:
                logger.info('Received SIGINT, trying to exit nicely. Press CTRL+C again to force exit.')
                for proc in process_pool:
                    proc.join()
                logger.info('All processes terminated! Nice!')
                exit(0)

            if finish:
                for _ in range(parallelization):
                    queue.put(TERMINATION_SENTINEL)
                return


@click.command()
@click.option('-a', '--api-key', envvar='API_KEY')
@click.option('-m', '--mastr-number', envvar='MASTR_NUMBER')
@click.option('-i', '--input', help='Input CSV files', multiple=True, type=click.Path(exists=True, readable=True))
@click.option('-n', '--index', type=click.INT,
              help='Index of column in input CSV where the Mastr\'s numbers are placed.')
@click.option('-p', '--parallelization', type=click.INT, default=multiprocessing.cpu_count(),
              help='Number of processes that should be spawn to increase speed of precessing. '
                   'Default is number of CPUs available.')
@click.option('-o', '--output', help='Folder where the final data will be written to',
              type=click.Path(file_okay=False, writable=True), default='data/')
def main(api_key, mastr_number, input, index, output, parallelization):
    """
    Script that will fetch detailed data of solar units specified by mastr numbers in the input CSV file and specific
    column of this CSV.
    """
    for input_file in input:
        logger.info(f'Processing file {input_file}')
        process_file(pathlib.Path(input_file), api_key, mastr_number, index, pathlib.Path(output), parallelization)


if __name__ == '__main__':
    main()

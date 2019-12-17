import os
import pathlib
import glob
import re
import psycopg2
from contextlib import contextmanager
import logging
from logging import basicConfig, exception, WARNING


class ProquestParser():

    @staticmethod
    def get_date(line):
        """[Gets date from ProQuest Congressional data]
        
        Arguments:
            line {[str]} -- [Current infile line]
        
        Returns:
            [str] -- [Returns date]
        """
        doc_date = re.compile(r'(Date: )((Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s*\d{2},\s*\d{4}\s*)')
        date_search = doc_date.search(line)

        if date_search:
            date = date_search.group(2)
        
        return date

    @staticmethod
    def get_url(line):
        """[Gets url from ProQuest Congressional data]
        
        Arguments:
            line {[str]} -- [Current infile line]
        
        Returns:
            [str] -- [Returns url]
        """
        doc_url = re.compile(r'(Permalink: )(.*)')
        url_search = doc_url.search(line)
        
        if url_search:
            url = url_search.group(2)

        return url


@contextmanager
def incoming(input_path):
    """[Opens read-only .txt file with encoding='Windows-1252']
    
    Arguments:
        input_path {[str]} -- [Input path]
    
    Yields:
        [obj] -- [Read-only .txt file with encoding='Windows-1252']
    """
    try:
        infile = open(input_path, 'r', encoding='Windows-1252') 
    except ConnectionError as incoming_error:
        exception(incoming_error)
    except Exception as incoming_e:
        exception(incoming_e)
    else:
        yield infile
    finally:
        infile.close()

@contextmanager
def outgoing(cred):
    """[Opens DB connection with PostgreSQL host='localhost']
    
    Arguments:
        cred {[str]} -- [Credentials]
    
    Yields:
        [obj] -- [DB connection with PostgreSQL host='localhost']
    """
    try:
        outfile = psycopg2.connect(user='postgres', password=cred[1], host='localhost', port='5432', database='postgres')
    except ConnectionError as outgoing_error:
        exception(outgoing_error)
    except Exception as outgoing_e:
        exception(outgoing_e)
    else:
        yield outfile
    finally:
        outfile.close()


def main():
    """[Driver of congress_parser.py]

    Setup:
        pp {[cls]} -- [ProquestParser() instance]
        cred {[str]} -- [Credentials]
        root_path {[str]} -- [Root path]
        congress_path {[str]} -- [Local path]
    """
    pp = ProquestParser()
    cred = pathlib.Path('login.bin').read_text().split('|')
    root_path = os.path.dirname(__file__)
    congress_path = os.path.join(root_path, 'data', 'congress')
    basicConfig(filename='cp.log', filemode='w', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=WARNING)

    date, url = None, None

    for filename in glob.glob(os.path.join(congress_path, '*.txt')):

        input_path = filename

        with incoming(input_path) as infile, outgoing(cred) as outfile:
            outfile_curs = outfile.cursor()
            line = infile.readline()
            
            while line:
                line = infile.readline()

                try:
                    date = pp.get_date(line)
                except:
                    pass

                try:
                    url = pp.get_url(line)
                except:
                    pass

                if None not in (date, url):
                    outfile_curs.execute('INSERT INTO congress_meta_tbl (pubdate, permalink) VALUES (%s, %s);', (date, url))
                    outfile.commit()
                    date, url = None, None
        
        outfile_curs.close()
    logging.shutdown()

if __name__ == '__main__':
    main()

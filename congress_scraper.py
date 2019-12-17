import pathlib
import psycopg2
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from contextlib import contextmanager
import logging
from logging import basicConfig, exception, WARNING


class ProquestScraper():
    
    @staticmethod
    def setup(driver, cred):
        """[Selenium login for ProQuest Congressional index]
        
        Arguments:
            cred {[str]} -- [Credentials]
        
        Returns:
            [bool] -- [Returns True]
        """
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'loginname')))
        except TimeoutException as timeout_login:
            exception(timeout_login)
        except Exception as login_e:
            exception(login_e)
        else:
            username = driver.find_element_by_id('loginname')
            password = driver.find_element_by_id('login')
            username.send_keys(cred[0])
            password.send_keys(cred[1])
            driver.find_element_by_class_name('psloginbutton').click()
        
        try:
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.NAME, 'accountOptions')))
        except TimeoutException as timeout_account:
            exception(timeout_account)
        except Exception as account_e:
            exception(account_e)
        else:
            select = Select(driver.find_element_by_name('accountOptions'))
            select.select_by_visible_text('WASHINGTON STATE UNIVERSITY')
            driver.find_element_by_id('submit').click()
        
        return True

    @staticmethod
    def get_soup(driver):
        """[Gets Beautiful Soup page source from ProQuest Congressional webpage]
        
        Arguments:
            driver {[obj]} -- [Selenium object]
        
        Returns:
            [obj] -- [Returns BeautifulSoup object]
        """
        try:
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'docsPageSpecific')))
        except TimeoutException as timeout_url:
            exception(timeout_url)
        except Exception as url_e:
            exception(url_e)
        else:
            return BeautifulSoup(driver.page_source, 'lxml')

    @staticmethod
    def get_text(driver, soup):
        """[Gets text from ProQuest Congressional webpage]
        
        Arguments:
            driver {[obj]} -- [Selenium object]
            soup {[obj]} -- [BeautifulSoup object]
        
        Returns:
            [str] -- [Returns text]
        """
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'segFull fulltext')))
        except TimeoutException as timeout_text:
            exception(timeout_text)
        except KeyError as key_error_text:
            exception(key_error_text)
        except Exception as text_e:
            exception(text_e)
        else:
            return soup.find('fulltext').get_text(' ')

    @staticmethod
    def get_title(driver, soup):
        """[Gets title from ProQuest Congressional webpage]
        
        Arguments:
            driver {[obj]} -- [Selenium object]
            soup {[obj]} -- [BeautifulSoup object]
        
        Returns:
            [str] -- [Returns title]
        """
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.docsContentRow > div:nth-child(1)')))
        except TimeoutException as timeout_title:
            exception(timeout_title)
        except KeyError as key_error_title:
            exception(key_error_title)
        except Exception as title_e:
            exception(title_e)
        else:
            return soup.select('.segColR')[0].text

    @staticmethod
    def get_committee(driver, soup):
        """[Gets committee from ProQuest Congressional webpage]
        
        Arguments:
            driver {[obj]} -- [Selenium object]
            soup {[obj]} -- [BeautifulSoup object]
        
        Returns:
            [str] -- [Returns committee]
        """
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.docSegGrid > div:nth-child(3)')))
        except TimeoutException as timeout_committee:
            exception(timeout_committee)
        except KeyError as key_error_committee:
            exception(key_error_committee)
        except Exception as committee_e:
            exception(committee_e)
        else:
            return soup.select('.segColR')[2].text

    @staticmethod
    def get_meta(driver, soup):
        """[Gets meta from ProQuest Congressional webpage]
        
        Arguments:
            driver {[obj]} -- [Selenium object]
            soup {[obj]} -- [BeautifulSoup object]
        
        Returns:
            [str] -- [Returns meta]
        """
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.docSegGrid > div:nth-child(4)')))
        except TimeoutException as timeout_meta:
            exception(timeout_meta)
        except KeyError as key_error_meta:
            exception(key_error_meta)
        except Exception as meta_e:
            exception(meta_e)
        else:
            return soup.select('.segColR')[3].text


@contextmanager
def incoming(cred):
    """[Opens DB connection with PostgreSQL host='localhost']
    
    Arguments:
        cred {[str]} -- [Credentials]
    
    Yields:
        [obj] -- [DB connection with PostgreSQL host='localhost']
    """
    try:
        infile = psycopg2.connect(user='postgres', password=cred[1], host='localhost', port='5432', database='postgres')
    except ConnectionError as incoming_error:
        exception(incoming_error)
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
    else:
        yield outfile
    finally:
        outfile.close()


def main():
    """[Driver of congress_parser.py]

    Setup:
        driver {[cls]} -- []
        ps {[cls]} -- [ProquestParser() instance]
        cred {[str]} -- [Credentials]
        news_path {[str]} -- [Local path]
    """
    driver = webdriver.Chrome()
    ps = ProquestScraper()
    cred = pathlib.Path('login.bin').read_text().split('|')
    basicConfig(filename='cs.log', filemode='w', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=WARNING)

    login = False

    with incoming(cred) as infile, outgoing(cred) as outfile:
        infile_cursor = infile.cursor()
        outfile_cursor = outfile.cursor()
        infile_cursor.execute('SELECT permalink from congress_meta_tbl;')
        
        for record in infile_cursor:
            driver.get(record[0])

            while login is False:
                login = ps.setup(driver, cred)

            soup = ps.get_soup(driver)
            
            try:
                outfile_cursor.execute('INSERT INTO congress_tbl (title, committee, meta, full_text, permalink) VALUES (%s, %s, %s, %s, %s);', (ps.get_title(driver, soup), ps.get_committee(driver, soup), ps.get_meta(driver, soup), ps.get_text(driver, soup), record[0]))
            except TypeError as insert_error:
                exception(insert_error)
            except Exception as insert_e:
                exception(insert_e)
            else:
                outfile.commit()

        infile_cursor.close()
        outfile_cursor.close()

    driver.quit()
    logging.shutdown()

if __name__ == '__main__':
    main()

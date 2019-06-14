#########################################################################################################
#   Program Name : NSW_OPAL_CARD_TYPE_LOAD.py                                                           #
#   Data Source: https://opendata.transport.nsw.gov.au/dataset/opal-trips-train                         #
#   Program Description:                                                                                #
#   This program prepares a SQLite table containing data about train opal card type usage in NSW.       #
#                                                                                                       #
#   Comment                                         Date                  Author                        #
#   ================================                ==========            ================              #
#   Initial Version                                 10/06/2019            Samson Leung                  #
#########################################################################################################
import sqlite3

#######################################################################
### Create NSW_TRAIN_OPAL_TRIPS Table                               ###
#######################################################################
conn = sqlite3.connect('NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019.sqlite')
cur = conn.cursor()

cur.executescript('''	
DROP TABLE IF EXISTS NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019;

CREATE TABLE NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019 (
	TRAIN_LINE        varchar(100),
	CARD_TYPE         varchar(100),
	PERIOD            varchar(100),
	COUNT             number(10)
);

''')

fname = 'Train Card Type_cleaned.txt'
fhand = open(fname)

#######################################################################
### Populate NSW_TRAIN_OPAL_CARD TYPES Table                        ###
#######################################################################
for line in fhand:
    fields = line.split('|')

    TRAIN_LINE = fields[0].strip()
    CARD_TYPE = fields[1].strip()
    PERIOD= fields[2].strip()
    COUNT = fields[3].strip()

    cur.execute('''INSERT INTO NSW_OPAL_CARD_TYPE_JULY_2016_APRIL_2019
        (
        TRAIN_LINE,
	    CARD_TYPE,
	    PERIOD,
	    COUNT
        )  
        VALUES ( ?, ?, ?, ?)''',
                (
                    TRAIN_LINE,
                    CARD_TYPE,
                    PERIOD,
                    COUNT
                ))

conn.commit()

print('Done')

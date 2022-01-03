# PI2CSV
 Export archive and snapshot time-series data from OSISoft PI WebApi to CSV file

## Dependencies
* Python 3.7
* OSISoft PI WebApi interface package installed

## Install and Run:
* git clone https://github.com/aperio-lab/PI2CSV.git
* pip install -r requirements.txt

## Usage
* python manage.py --host MyPIHost.com --user piuser --password pipassword --name_filter tag_name\* --limit 150000 

Alternatively, host, user and password can be defined in **conf/resources/config/config-default.yml**
* python manage.py --name_filter tag_name\* --limit 150000 --data_type n_samples_archive --export_dir ../DataExport


## Options
* --host: PI WEBAPI IP/Host address
* --user: PI user
* --password: PI password
* --log: Set logger name
* --limit: Num of last samples to get from PI archive for each tag, DEFAULT=150000
* --to: Date-Time Timestamp to get archive data until, default is NOW. DEFAULT=NOW
* --data_type: Type of data to get from PI: archive/snapshot. DEFAULT=n_samples_archive
* --name_filter: Tag name filter, use whilecard for pattern search. DEFAULT=None
* --data_server: PI Data Server web_id for tags search and data export. DEFAULT=None
* --date_format: Date format to use when writing time-series to CSV. DEFAULT='%Y-%m-%d %H:%M:%S'
* --export_dir: Directory path for CSV export. DEFAULT=os.path.realpath(os.path.dirname(__file__))

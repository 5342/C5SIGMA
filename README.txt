C5 SIGMA -- Copyright (C) Command Five Pty Ltd 2011
<http://www.commandfive.com/>

C5 SIGMA is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

C5 SIGMA is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with C5 SIGMA. If not, see <http://www.gnu.org/licenses/>.



README
------



Table of Contents
-----------------

1. System Requirements
2. Usage
3. Data Filters
4. Fixups
5. Changelog
6. MySQL .NET Connector
7. Feedback and Bug Reports



1. System Requirements
----------------------

Software:

- Windows Operating System
- .NET 4.0 Runtime
- Either:
	- Microsoft SQL Server 2008 (or later) [RECOMMENDED]
	- MySQL 5.5 (or later)
- Wireshark 1.6.0 (or later)

Hardware:

- A platform capable of running TShark (Wireshark)
- A platform capable of running either Microsoft SQL Server 2008 or MySQL 5.5.



2. Usage
--------

SIGMA is a command line application. It invokes TShark (a component of 
Wireshark) on each file found in an input directory, creating a temporary 
XML output file. SIGMA then processes and loads the resulting XML files 
into a SQL database which can be located either on-box or off-box.

The SQL database schema is generated automatically by SIGMA as data is 
processed. This allows SIGMA to support any dissector (protocol processing 
module) compatible with Wireshark. 

By default a large number of rows are created in the database for each 
packet found in the data being processed. The number of rows created can be 
reduced either by use of a data filter (described in XML) or TShark 
(Wireshark) filters (see command line option "--tsharkparams").

Currently only one preset data filter ("Basic") is provided (see command 
line option "--datafilterpreset"). The "Basic" filter restricts output to
include only the following subset of supported protocols:


ARP, Ethernet, IP, TCP, UDP, ICMP, DNS, HTTP, FTP, TFTP, SMTP, POP, IMAP, 
IMF, SSL, SSH, Telnet, X11, UMTS, DHCP, NNTP, SMB, NetBios, rexec, MSN, 
IRC, and RDP (X.224 & TPKT).


Custom data filters can be provided by specifying an XML file containing
rules (see command line option "--datafilter"). A sample data filter is 
provided in section 3 of this document.

Executing SIGMA with no command line options or the "--help" option 
displays the following help text:


Usage: SIGMA.exe
    --dbhostname|-dbh <dbhvalue>
    [--datafilter|-fil <filvalue>]
    [--datafilterpreset|-pre <prevalue>]
    [--dbcatalog|-dbc <dbcvalue>]
    [--dbforeignkeys|-dbfk]
    [--dbintegrated|-dbi]
    [--dbmysql|-dbmy]
    [--dbpassword|-dbp <dbpvalue>]
    [--dbsqlserver|-dbms]
    [--dbusername|-dbu <dbuvalue>]
    [--fixups|-fix <fixvalue>]
    [--help|-h]
    [--inputpath|-in <invalue>]
    [--outputpath|-out <outvalue>]
    [--tshark|-ts <tsvalue>]
    [--tsharkparams|-tsp <tspvalue>]

--dbhostname <value> (-dbh)
    Database hostname.

--datafilter <value> (-fil)
    Path to an XML formatted data filter file.

--datafilterpreset <value> (-pre)
    Name of a preset (built-in) data filter. Presets: Basic.

--dbcatalog <value> (-dbc)
    Database catalog/schema name.

--dbforeignkeys (-dbfk)
    Disable generation of foreign keys in the database.

--dbintegrated (-dbi)
    Use integrated Windows authentication with the database.

--dbmysql (-dbmy)
    Use MySQL database.

--dbpassword <value> (-dbp)
    Database password. Not valid with '--dbintegrated'.

--dbsqlserver (-dbms)
    Use SQL Server database. Default.

--dbusername <value> (-dbu)
    Database username. Not valid with '--dbintegrated'.

--fixups <value> (-fix)
    Path to an XML formatted fixups file.

--help (-h)
    Display this usage information.

--inputpath <value> (-in)
    Path to a directory containing network capture files.

--outputpath <value> (-out)
    Path to a directory that will receive processed data files.

--tshark <value> (-ts)
    Path to 'tshark.exe' (including the filename).

--tsharkparams <value> (-tsp)
    Additional parameters to pass to 'tshark.exe'.


Example usage:


SIGMA.exe 
  -in ".\Test\Input" 
  -out ".\Test\Output" 
  -dbh "localhost" 
  -dbi 
  -ts "C:\Program Files\Wireshark\tshark.exe" 
  -pre Basic


This command processes all the files (regardless of extension) located in 
the ".\Test\Input" directory, producing output files in the directory
".\Test\Output". Paths here are relative to the current directory. 

Output files are processed and loaded into the Microsoft SQL Server 
instance running on "localhost" (the local machine). Connections to SQL 
Server are authenticated using Integrated Authentication (i.e. no 
credentials are required as long as the current Windows user has been 
configured for access to the database). 

Since no catalog name is specified the default database/catalog "SIGMA" 
will be used (or created if it does not exist).

The built-in filter set "Basic" (currently the only one available) is used
to reduce the number of tables created in the database.



3. Data Filters
---------------

Data filters are defined in XML format and passed to SIGMA using the command 
line option "--datafilter". Tables & columns are allowed to appear in the 
output dataset by specifying .NET style regular expressions that are matched 
against table names and column names.

In the sample file that follows all tables are blocked from appearing in 
the output dataset except those tables starting with "eth", "ip", "tcp", 
and "udp". The "geninfo" table is also specifically allowed. Tables always 
start with the short name of the Wireshark dissector (protocol) they relate 
to - so the sample filter below only allows protocols ETH, IP, TCP, and UDP.

Note: IPv6 is implicitly allowed since IPv6 table names start with "ip".


<?xml version="1.0" encoding="utf-8" ?>
<filter>

  <tables>

    <!-- Default Deny -->
    <deny tableName=".*" />

    <allow tableName="^geninfo$" />
    <allow tableName="^eth.*$" />
    <allow tableName="^ip.*$" />
    <allow tableName="^tcp.*$" />
    <allow tableName="^udp.*$" />

  </tables>
  
  <columns>

    <!-- Default Allow -->
    <allow tableName=".*" columnName=".*" />

  </columns>

</filter>



4. Fixups
---------

Fixups are used to generate sensical names for otherwise unnamed fields 
generated by TShark. The built-in fixups are usually sufficient for this 
purpose. The built-in fixup file is called "Fixups.xml" and is available
as part of the SIGMA source code - it gets compressed and stored inside 
the SIGMA binary each time the binary is recompiled.

The built-in fixups file is a very large XML file that is mostly generated 
automatically by scanning the Wireshark source code with a special tool: 
SIGMASourceScan. SIGMASourceScan is included as part of the SIGMA release 
along with SIGMACompress (the tool used to automatically compress the file 
during the build process).

A few manual fixups are also included in the built-in fixups file. They are 
copied below for reference:


<?xml version="1.0" encoding="utf-8" ?>
<fixups>

  <!-- Manual Template Fixups -->

  <template parentName=".*" name="" show="^.+: [tT]ype .+, [cC]lass .+$" 
    showname=".*" value=".*" nameFormat="$(parentNamePrefix)namespec" 
    valueFormat="$(value)"/>

  <template parentName=".*" name="" show="^SEQ/ACK analysis$" showname=".*" 
    value=".*" nameFormat="tcp.analysis" valueFormat="$(value)" />

  <template parentName=".*" name="" show="^.*HTTP/[0-9]\.[0-9].*$" 
    showname=".*" value=".*" nameFormat="$(parentNamePrefix)httpdata" 
    valueFormat="$(value)"/>

</fixups>



5. Changelog
------------

* Version 1.1.0.0 [CURRENT]

  * Feature: Added support for MySQL database engine (command line option 
  "--dbmysql"). Performance is much better on SQL Server.

  * Feature: Added a new command line option to disable automatic generation 
  of foreign keys (significantly improves performance on MySQL).

  * Bug fix: Fixed command line parameter parser so that username/password 
  (basic) authentication can be used.

  * Bug fix: Fixed command line parameter parser so that default values for 
  paths ("-in", "-out", "-ts") are used when the parameters aren't specified.
 
* Version 1.0.0.0

  * Initial release.



6. MySQL .NET Connector
-----------------------

SIGMA uses an unmodified version of the "MySQL Connector/Net 6.x" component 
for interaction with the MySQL database engine. This component is provided 
by Oracle and used here under GPLv2. The full licence is available in a file 
called "COPYING" within the SIGMA source code distribution. 

Source and binary distributions of the connector are available for download 
from <http://dev.mysql.com/downloads/>.

"MySQL Connector/Net 6.x

This is a release of MySQL Connector/Net, Oracle's dual-
license ADO.Net Driver for MySQL. For the avoidance of
doubt, this particular copy of the software is released
under the version 2 of the GNU General Public License.
MySQL Connector/Net is brought to you by Oracle.

Copyright (c) 2004, 2011, Oracle and/or its affiliates. All rights reserved."



7. Feedback and Bug Reports
---------------------------

Please visit our website <http://www.commandfive.com/> for the latest 
binaries and source code. Feedback, bug reports, and feature requests 
should be directed to <sigma@commandfive.com>.

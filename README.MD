#Photo Noncer

A project to inspect picture files, read timestamps and other information from them, and organise/update the files correspondingly.

The single script `photnon.py` should be able to run as a one step or a two step process:

1. Data mining from one folder and generation of datafile
2. Analysis of datafile (with minor user input) and generation of scripts for further review before off-band execution.

## Potential issues

As the data extraction and the analysis are two independent steps, it could happen that datafiles generated on different systems are analysed together. It could happen that:

* PREVENTABLE in code: Same data, mounted in the same path on two different machines, are combined. Duplicates would be detected and one file removed but, when running, that would be the only copy, it would match the equality checks and so deleted.
* Same data, mounted on different paths on two different machines, are combined. Duplicates would be detected, but deletion script will fail if only one folder is available on the script execution machine. Or delete all data if the same data is mounted.
* Different data, mounted on the same path on two different machines (or even the same machine), are combined. UM....

ESSENTIALLY, USE EXTERNAL HDs NAMED DIFFERENTLY, OR BE CONSISTENT WITH FOLDER NAMES ACROSS MACHINES. OR USE A SINGLE MACHINE.
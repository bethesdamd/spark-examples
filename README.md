Stand up Apache Spark on any machine using Vagrant.  Also includes some sample jobs for Word Count, and for log file processing.

Vagrant is a front-end for VirtualBox, and allows the easy creation and launching of virtual machines on any platform.  The included VagrantFile here will create and completely configure a Linux VM with Apache Spark.

Please see the Vagrant site for further details about Vagrant.

Dependencies:

[Install Vagrant](http://www.vagrantup.com/downloads.html)
[Install VirtualBox](https://www.virtualbox.org/wiki/Downloads)

Clone this repository
Run 'vagrant up'.  This will take a while as software is downloaded and installed.  You will then have a running VM but you won't be connected to it yet.
Run 'vagrant ssh' to connect to your VM


These jobs are designed to be run from within the spark console (as opposed to using spark-submit).

Vagrant configuration information is from: http://thegrimmscientist.com/2014/12/01/vagrant-tutorial-spark-in-a-vm/ which references 
Spark version 1.1.  All 1.1 references in that blog post should be changed to the current Spark version which as of 
Feb 2015 is 1.2.  





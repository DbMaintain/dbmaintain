[![Build Status](https://travis-ci.org/DbMaintain/dbmaintain.svg?branch=master)](https://travis-ci.org/DbMaintain/dbmaintain)
[![Maintainability](https://api.codeclimate.com/v1/badges/b97161b004c5592178f0/maintainability)](https://codeclimate.com/github/DbMaintain/dbmaintain/maintainability)
[![Maven Central Version](https://img.shields.io/maven-central/v/org.dbmaintain/dbmaintain.svg)](http://search.maven.org/#search|gav|1|g:"org.dbmaintain"%20AND%20a:"dbmaintain")

DbMaintain
==========

Fork of [DbMaintain](http://www.dbmaintain.org/overview.html) with new Features.

Overview
-------------

DbMaintain enables automatic roll-out of updates to a relational database. It brings database scripts into version control just like regular source code to transparently deploy databases from development to production.

DbMaintain keeps track of which database updates have been deployed on which database.

Updates are performed incrementally: Only what has been changed since the last deployment is applied. Features such as repeatable scripts, postprocessing scripts, multi-database and database user support and support for patches turn DbMaintain into a complete solution for the enterprise.

Documentation
-------------
For usage with docker see [docker configuration](https://dbmaintain.github.io/docs/docker/).

See [documentation](https://dbmaintain.github.io/docs/) for getting started and full documentation. 

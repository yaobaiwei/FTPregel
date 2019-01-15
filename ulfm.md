ULFM INSTALL GUIDE
===================

### GET ULFM

The latest ULFM can be obtained from [https://bitbucket.org/icldistcomp/ulfm/downloads](https://bitbucket.org/icldistcomp/ulfm/downloads). There are installation and deploy guide contained in the downloaded package. However, we also provided a sightly older version that we actually used to build FT-Pregel, released in Dec 2014. The package also contains fixes for some bugs(these fixes were later merged into ULFM).

### BUILD ULFM

Before you build ULFM, please make sure that you get fairly recent versions of autoconf, automake, m4 and libtool. For our platform we use autoconf-2.69, automake-1.14, m4-1.4.17 and libtool-2.4.5. After extracting the package, issue the following commands,

    ./autogen.pl
    ./configure
    make -j8
    sudo make install

### DEPLOY ULFM

For cluster environment, ULFM need to be installed on each compute node.

# Troubleshooting

## Host Dependencies Missing

The `make test-setup` script will check for all required host dependencies, and report any missing ones. In this context, "dependency" refers to programs that run independently on the host system.

For example

```shell
% make test-setup         
sh scripts/test-setup.sh
error: host dependency 'wget' not found
make: *** [test-setup] Error 1
% 
```

The error message indicates that the `wget` program could not be found. In this case, you should install wget. After running `make test-setup` again, additional host dependencies may be found to be missing.

Generally, we can't provide instructions to install each dependency on your host machine, as there are many systems and system package managers.

For example, on macOS using "macports" as a package manager:

```shell
sudo port install wget

% sudo port install wget
Password:
--->  Computing dependencies for wget
The following dependencies will be installed: 
 gnutls
 libtasn1
 nettle
 p11-kit
Continue? [Y/n]: y
--->  Fetching archive for libtasn1
--->  Attempting to fetch libtasn1-4.18.0_0.darwin_21.x86_64.tbz2 from https://packages.macports.org/libtasn1
--->  Attempting to fetch libtasn1-4.18.0_0.darwin_21.x86_64.tbz2.rmd160 from https://packages.macports.org/libtasn1
--->  Installing libtasn1 @4.18.0_0
--->  Activating libtasn1 @4.18.0_0
--->  Cleaning libtasn1
... elided for brevity ...
--->  No broken files found.                             
--->  No broken ports found.
--->  Some of the ports you installed have notes:
  wget has the following notes:
    To customize wget, you can copy /opt/local/etc/wgetrc.sample to /opt/local/etc/wgetrc and then make changes.
% 
```

Host requirements are documented in the script `scripts/test-setup.sh` and include:

- wget
- java 1.8 (other versions?)
- docker
- python 3.7
- pipenv
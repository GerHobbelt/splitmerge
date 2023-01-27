
#include <stdio.h>
#include <stdlib.h>

#include <iostream>


static int usage()
{
	const char *msg = R"xxx(
splitmerge <options> <sourcefile>

Usage
-----

Split a file in smaller files or merge files into a larger one.

When a file is split, the resulting fragments will have the same
path & name as the original file, but '.seg0000' will be appended
as the file extension for the first segment. Subsequent segments
will have .seg0001, .srg0002, ... as file extension.

Default baheviour of the tool is to SPLIT, UNLESS the specified
source file has extension .seg0000, in which case the default
action is to MERGE.

The segment files all include a small header, which records
the original file size and its (BLAKE3) hash, which is used to
verify any future merge action by checking the hash of the
merge result file.


Options
-------

-s   Split (forced: you only need this when you want to split
     a *sourcefile* that itself has the file extension .seg0000
     You don't neeed this command/option for any other file as
     the DEFAULT ACTION is to SPLIT.)

Note: MERGE is chosen as the default action automatically when
the sourcefile specified has extension .seg0000 (or any other
.segXXXX number: the tool will start merging at .seg0000 anyhow).

-k   Keep the source. Default action is to DELETE the source
     once the action has completed successfully. This applies
     to both SPLIT and MERGE: SPLIT will, by default, delete
     the source file after splitting; MERGE will delete all
     segmeent files after the merge has completed sucessfully.

-l <size>
     Specify the segment size in bytes (default), kilobytes (k)
     or megabytes (M), e.g.: -l 42M

     Default sizee is: 15M


Segment Files
-------------

The .seg0000, .seg0001, ... segment files contain the original
file data verbatim (no compression is attempted), while prefixed
by a small header, which lists a few useful metadata items which
are used to verify the SPLIT+MERGE process:

- original file size (in bytes)
- original file's hash (BLAKE3 fingerprint)


Example Usage
-------------

splitmerge largeFile.bin

--> largeFile.bin is split into 15M segments named
largeFile.bin.seg0000, largeFile.bin.seg0001, etc., while
largeFile.bin itself will be deleted at the end of the
split action.

splitmerge -l 100M largeFile.bin

--> ditto as the above, but segments are now sized at
100 Mbyte each (instead of the default 15MB).

splitmerge largeFile.bin.spec0000

--> largeFile.bin is reconstructed ('merged') from the
series of segment files: splitmerge will expect all these
segment files to be present in the same directory as the
initial .seg0000 file.
After merging the segments, the reconstructed 'original'
largeFile.bin file is hashed and compared against the
stored hash in .seg0000: the meerge/reconstruction action
is successful when the hashes match, after which the segment
files will be deleted.

splitmerge -k largeFile.bin.spec0000

--> ditto as the above, but now all the segment files are
kept, alongside the reconstructed largeFile.bin file.

)xxx";
	fprintf(stderr, "%s", msg);
	return EXIT_FAILURE;
}


// stored on disk in Little Endian format:
typedef struct info_header
{
	int64_t format_version;
	int64_t filesize;
	uint8_t hashbytes[32];

	// reserved for future use.
	uint8_t unused[256 - 32 - 8];
} info_header_t;


int main(int argc, const char **argv)
{
	if (argc <= 1)
		return usage();

	bool keep = false;
	int64_t split_size = 15000000;

	return EXIT_SUCCESS;
}

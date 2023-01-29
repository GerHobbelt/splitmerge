

#include "mupdf/mutool.h"
#include "mupdf/fitz.h"
#include "mupdf/helpers/dir.h"
#include "mupdf/helpers/system-header-files.h"

#include "blake3.h"
#include "endianness.h"

#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#if defined(_WIN32)
#include <windows.h>
#endif

#include <string>
#include <vector>

using namespace std;

#undef MIN
#define MIN(a, b)  ((a) <= (b) ? (a) : (b))


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
	fz_info(fz_get_global_context(), "%s", msg);
	return EXIT_FAILURE;
}


#if BLAKE3_OUT_LEN != 32
#error "B0RK! BLAKE3 hashes MUST be 32 bytes wide! This is WRONG, man! W-w-w-wrong!!!1!"
#endif


// stored on disk in Little Endian format:
typedef struct info_header
{
	int32_t format_version;
	int32_t segment_number;    // index number of this segment; starts with 1 (!)
	uint64_t filesize;
	uint8_t hashbytes[32];

	// reserved for future use.
	uint8_t unused[256 - 32 - 16];
} info_header_t;






#ifdef _MSC_VER
#define main main_utf8
#endif

int main(int argc, const char** argv)
{
	fz_context* ctx = NULL;

	if (!fz_has_global_context())
	{
		ctx = fz_new_context(NULL, NULL, FZ_STORE_UNLIMITED);
		if (!ctx)
		{
			fz_error(ctx, "cannot initialise MuPDF context");
			return EXIT_FAILURE;
		}
		fz_set_global_context(ctx);
	}

	ctx = fz_new_context(NULL, NULL, FZ_STORE_DEFAULT);
	if (!ctx)
	{
		fz_error(ctx, "cannot initialise MuPDF context");
		return EXIT_FAILURE;
	}

	if (argc == 0)
	{
		fz_error(ctx, "No command name found!");
		return EXIT_FAILURE;
	}

	bool keep = false;
	bool split_forced = false;
	int64_t split_size = 15000000;

	int c;
	int cli_count = 0;

	fz_getopt_reset();
	while ((c = fz_getopt(argc, argv, "hskl:")) != -1)
	{
		cli_count++;
		switch (c)
		{
		case 'l': {
			char *unit_str = NULL;
			split_size = strtol(fz_optarg, &unit_str, 10);
			switch (toupper(*unit_str))
			{
			case 0:
				break;

			case 'B':
				unit_str++;
				break;

			case 'K':
				split_size *= 1024;
				unit_str++;
				break;

			case 'M':
				split_size += 1024 * 1024;
				unit_str++;
				break;

			default:
				break;
			}
			if (*unit_str)
			{
				fz_error(ctx, "split-size unit MUST be B, K or M. Unsupported unit: %s\n", unit_str);
				return EXIT_FAILURE;
			}
		}
			break;

		case 'k': keep = true; break;
		case 's': split_forced = true; break;
		case 'h':
		default:
			return usage();
		}
	}

	if (fz_optind >= argc)
	{
		if (cli_count)
		{
			fz_error(ctx, "ERROR: No source file(s) specified to split.\n\n\n");
		}
		return usage();
	}

	int count_done = 0;
	int count_skipped = 0;

	unsigned char buf[65536];

	while (fz_optind < argc)
	{
		const char *filepath = argv[fz_optind++];

		const char *filename = fz_basename(filepath);
		const char *file_ext = fz_name_extension(filename);

		// we accept any of the segments' filenames, hence we must convert its extension to
		// properly match .seg0000 :
		// 
		// ..SEG1234 --> .SEG0000
		char *ext2test = fz_strdup(ctx, file_ext);
		if (0 == fz_strncasecmp(file_ext, ".seg0000", 4))
		{
			char *p = ext2test + strlen(ext2test) - 1;
			for (; p >= ext2test && isdigit(*p); p--)
				*p = '0';
			// SEG123456789 --> SEG1234 ~ SEG0000
			if (p - ext2test < 5 && strlen(ext2test) > 8)
				ext2test[8] = 0;
		}

		// split or merge?
		bool split = (split_forced || 0 != fz_strcasecmp(ext2test, ".seg0000"));

		fz_free(ctx, ext2test);

		if (split)
		{
			fz_stream* datafeed = NULL;
			char *seg_filepath = NULL;
			fz_output *dest_file = NULL;
			vector<string> produced_output_files;

			fz_try(ctx)
			{
				struct stat st = {0};
				if (stat(filepath, &st))
				{
					int err = errno;
					fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot inspect source file %q: error %d (%s)\n", filepath, err, strerror(err));
				}

				uint64_t filesize = st.st_size;

				// no need to split when the file is smaller than the specified segment size!
				if (filesize <= split_size)
				{
					fz_info(ctx, "No need to split file %q as it would only occupy a single segment anyway: filesize: %zu vs. segment size: %zu.\n", filepath, (size_t)filesize, (size_t)split_size);

					count_done++;
					count_skipped++;
				}
				else
				{
					datafeed = fz_open_file(ctx, filepath);
					if (datafeed == NULL)
					{
						int err = errno;
						fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot open source file %q: error %d (%s)\n", filepath, err, strerror(err));
					}

					// calculate the hash/fingerprint of the source file:

					// Initialize the hasher.
					blake3_hasher hasher;
					blake3_hasher_init(&hasher);

					// Read input bytes from file.
					size_t n = fz_read(ctx, datafeed, buf, sizeof(buf));
					while (n > 0)
					{
						blake3_hasher_update(&hasher, buf, n);
						n = fz_read(ctx, datafeed, buf, sizeof(buf));
					}

					// Finalize the hash. BLAKE3_OUT_LEN is the default output length, 32 bytes.
					uint8_t hash[BLAKE3_OUT_LEN];
					blake3_hasher_finalize(&hasher, hash, BLAKE3_OUT_LEN);

					fz_seek(ctx, datafeed, 0, SEEK_SET);

					// prep a header block:
					int segment_index = 0;

					info_header_t seg_hdr =
					{
						1,		// version
						0,
						filesize,
					};
					memcpy(seg_hdr.hashbytes, hash, BLAKE3_OUT_LEN);

					int64_t copy_len = filesize;
					while (copy_len > 0)
					{
						seg_hdr.segment_number = segment_index + 1;   // segment number in the file header is 1-based on purpose!

						// construct filename for each segment file:
						seg_filepath = fz_asprintf(ctx, "%s.seg%04d", filepath, segment_index);
						if (!seg_filepath)
						{
							fz_throw(ctx, FZ_ERROR_ABORT, "Out of memory while setting up segment file(s) for source file %q\n", filepath);
						}

						produced_output_files.push_back(seg_filepath);

						dest_file = fz_new_output_with_path(ctx, seg_filepath, FALSE);

#if !defined(L_LITTLE_ENDIAN)
						fz_write_int32_le(ctx, dest_file, 1);
						fz_write_int32_le(ctx, dest_file, segment_index);
						fz_write_int64_le(ctx, dest_file, filesize);
						fz_write_data(ctx, dest_file, hash, BLAKE3_OUT_LEN);
						uint8_t reserved_nuls[256 - 32 - 16] ={0};
						fz_write_data(ctx, dest_file, reserved_nuls, sizeof(reserved_nuls));
#else
						fz_write_data(ctx, dest_file, &seg_hdr, sizeof(seg_hdr));
#endif

						// now write the segment's data:
						size_t segment_size = MIN(split_size, copy_len);
						size_t seg_copy_size = segment_size;
						size_t read_size = MIN(sizeof(buf), seg_copy_size);
						size_t n = fz_read(ctx, datafeed, buf, read_size);
						while (n > 0)
						{
							fz_write_data(ctx, dest_file, buf, n);
							seg_copy_size -= n;
							read_size = MIN(sizeof(buf), seg_copy_size);
							if (read_size > 0)
								n = fz_read(ctx, datafeed, buf, read_size);
							else
								n = 0;
						}

						// has the entire designated segment been copied?
						if (seg_copy_size != 0)
						{
							fz_throw(ctx, FZ_ERROR_ABORT, "Failed to complete writing to segment file %q; aborting!\n", seg_filepath);
						}

						fz_close_output(ctx, dest_file);
						fz_drop_output(ctx, dest_file);
						dest_file = NULL;

						copy_len -= segment_size;
						segment_index++;
					}

					// all done: close source file
					fz_drop_stream(ctx, datafeed);
					datafeed = NULL;

					fz_info(ctx, "OK split: %q @ %zu bytes --> %d segment files.\n", filepath, (size_t)filesize, segment_index);

					// house-cleaning?
					if (!keep)
					{
						// delete source file
						if (fz_remove_utf8(ctx, filepath))
						{
							fz_error(ctx, "Error reported while attempting to delete the source file %q: %s\n", filepath, fz_ctx_pop_system_errormsg(ctx));
							// do not throw an error, as this is considered benign, as in: not suitable for full cleanup, which would nuke all generated segment files (*!OOPS!* when the delete of the sourcefile made it even 'half-way through'...  :-(  )
						}

						fz_info(ctx, "Clean-Up: source file has been deleted.\n");
					}
				}
			}
			fz_catch(ctx)
			{
				if (datafeed)
				{
					fz_drop_stream(ctx, datafeed);
					datafeed = NULL;
				}

				if (dest_file)
				{
					fz_drop_output(ctx, dest_file);
					dest_file = NULL;
				}

				fz_error(ctx, "Failure while splitting %q: %s", filepath, fz_caught_message(ctx));

				// house cleaning
				for (auto segmentfilepath : produced_output_files)
				{
					// delete segment file
					(void)fz_remove_utf8(ctx, segmentfilepath.c_str());
				}

				fz_info(ctx, "Clean-Up: Generated segment files have been deleted.\n");
			}
		}
		else
		{
			// MERGE
			fz_stream* datafeed = NULL;
			char *seg_filepath = NULL;
			char *dest_filepath = NULL;
			fz_output *dest_file = NULL;
			int segment_index = 0;
			info_header_t seg_hdr = {0};
			uint64_t dest_filesize = 0;
			vector<string> processed_segment_files;

			fz_try(ctx)
			{
				// construct the filename for the initial segment file:
				// (allocate spare space for what we're about to do next; assume we'll never have more than a billion segment files ;-) )
				seg_filepath = fz_asprintf(ctx, "%sXSEG0000000000", filepath);
				if (!seg_filepath)
				{
					fz_throw(ctx, FZ_ERROR_ABORT, "Out of memory while setting up merge action for segment file %q\n", filepath);
				}

				const char *fext = fz_name_extension(seg_filepath);

				do
				{
					// since the space of the filepath is allocated and of sufficient size, we can simply plug in the correct extension:
					fz_snprintf((char *)fext, 10, ".seg%04d", segment_index);

					processed_segment_files.push_back(seg_filepath);

					// open the little bugger and loaad the header for inspection:
					datafeed = fz_open_file(ctx, seg_filepath);
					if (datafeed == NULL)
					{
						int err = errno;
						fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot open segment file %q: error %d (%s)\n", seg_filepath, err, strerror(err));
					}

					size_t n;

#if !defined(L_LITTLE_ENDIAN)
					fz_read_int32_le(ctx, dest_file, 1);
					fz_read_int32_le(ctx, dest_file, segment_index);
					fz_read_int64_le(ctx, dest_file, filesize);
					fz_read_data(ctx, dest_file, hash, BLAKE3_OUT_LEN);
					uint8_t reserved_nuls[256 - 32 - 16] ={0};
					n = fz_read(ctx, dest_file, reserved_nuls, sizeof(reserved_nuls));
#else
					n = fz_read(ctx, datafeed, (uint8_t *)&seg_hdr, sizeof(seg_hdr));
#endif
					if (n != sizeof(info_header_t))
					{
						fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot read segment file %q: not enough data: invalid info header.\n", seg_filepath);
					}

					// check the header:
					if (seg_hdr.format_version != 1)
					{
						fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot process segment file %q: unsupported format: unknown version reported in the header.\n", seg_filepath);
					}
					if (seg_hdr.segment_number != segment_index + 1)
					{
						fz_throw(ctx, FZ_ERROR_GENERIC, "Failed to process segment file %q: unexpected segment number %d does not match our expectations (~ segment number %d)\n", seg_filepath, (int)seg_hdr.segment_number, segment_index + 1);
					}

					// only create the destination file the first time we go through this loop:
					if (segment_index == 0)
					{
						// now go create the target file:
						dest_filepath = fz_strdup(ctx, filepath);
						if (!dest_filepath)
						{
							fz_throw(ctx, FZ_ERROR_ABORT, "Out of memory while setting up merge action for segment file %q\n", filepath);
						}

						const char *dest_fext = fz_name_extension(dest_filepath);
						*((char *)dest_fext) = 0;

						dest_file = fz_new_output_with_path(ctx, dest_filepath, FALSE);
						if (dest_file == NULL)
						{
							int err = errno;
							fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot open destination file %q: error %d (%s)\n", dest_filepath, err, strerror(err));
						}
					}

					// now write the segment's data:
					size_t seg_copy_size = 0;
					size_t read_size = sizeof(buf);
					n = fz_read(ctx, datafeed, buf, read_size);
					while (n > 0)
					{
						fz_write_data(ctx, dest_file, buf, n);
						seg_copy_size += n;
						n = fz_read(ctx, datafeed, buf, read_size);
					}

					// has the entire designated segment been copied?
					if (n != 0)
					{
						fz_throw(ctx, FZ_ERROR_ABORT, "Failed to complete reading segment file %q; aborting!\n", seg_filepath);
					}

					fz_drop_stream(ctx, datafeed);
					datafeed = NULL;

					segment_index++;

					// now see if there should be another segment file ready for us:
					dest_filesize += seg_copy_size;

				} while (dest_filesize < seg_hdr.filesize);

				if (dest_filesize != seg_hdr.filesize)
				{
					fz_throw(ctx, FZ_ERROR_ABORT, "Failed to reconstruct the source file %q from the segments: file size does not match the expectation!\n", dest_filepath);
				}

				// now check the destination file by calculating its fingerprint/checksum:
				fz_close_output(ctx, dest_file);
				fz_drop_output(ctx, dest_file);
				dest_file = NULL;

				datafeed = fz_open_file(ctx, dest_filepath);
				if (datafeed == NULL)
				{
					int err = errno;
					fz_throw(ctx, FZ_ERROR_GENERIC, "Cannot open target file %q: error %d (%s)\n", dest_filepath, err, strerror(err));
				}

				// calculate the hash/fingerprint of the target file:

				// Initialize the hasher.
				blake3_hasher hasher;
				blake3_hasher_init(&hasher);

				// Read input bytes from file.
				size_t n = fz_read(ctx, datafeed, buf, sizeof(buf));
				while (n > 0)
				{
					blake3_hasher_update(&hasher, buf, n);
					n = fz_read(ctx, datafeed, buf, sizeof(buf));
				}

				// Finalize the hash. BLAKE3_OUT_LEN is the default output length, 32 bytes.
				uint8_t hash[BLAKE3_OUT_LEN];
				blake3_hasher_finalize(&hasher, hash, BLAKE3_OUT_LEN);

				fz_drop_stream(ctx, datafeed);
				datafeed = NULL;

				// compare the fingerprint:
				if (0 != memcmp(seg_hdr.hashbytes, hash, sizeof(hash)))
				{
					fz_throw(ctx, FZ_ERROR_ABORT, "Failed to reconstruct the source file %q from the segments: file hash/fingerprint does not match the expectation!\n", dest_filepath);
				}

				fz_info(ctx, "OK merge/reconstruct: %q @ %zu bytes <-- %d segment files.\n", dest_filepath, (size_t)seg_hdr.filesize, segment_index);

				// house cleaning?
				if (!keep)
				{
					for (auto segmentfilepath : processed_segment_files)
					{
						// delete segment file
						if (fz_remove_utf8(ctx, segmentfilepath.c_str()))
						{
							fz_error(ctx, "Error reported while attempting to delete the source segment file %q: %s\n", segmentfilepath.c_str(), fz_ctx_pop_system_errormsg(ctx));
							// do not throw an error, as this is considered benign, as in: not suitable for full cleanup, which would nuke the generated = reconstructed original file (*!OOPS!*: dest file gone and now some or all source segments are nuked too? Very much OOPSIE!  :-(  )
						}
					}

					fz_info(ctx, "Clean-Up: All source segment files have been deleted.\n");
				}
			}
			fz_catch(ctx)
			{
				if (datafeed)
				{
					fz_drop_stream(ctx, datafeed);
					datafeed = NULL;
				}

				if (dest_file)
				{
					fz_drop_output(ctx, dest_file);
					dest_file = NULL;
				}

				fz_error(ctx, "Failure while processing %q et al: %s", filepath, fz_caught_message(ctx));

				// house cleaning
				(void)fz_remove_utf8(ctx, dest_filepath);

				fz_info(ctx, "Clean-Up: Destination file (damaged) has been deleted.\n");
			}
		}

		fz_flush_warnings(ctx);
	}

	fz_drop_context(ctx);

	return EXIT_SUCCESS;
}

#ifdef _MSC_VER
int wmain(int argc, const wchar_t *wargv[])
{
	const char **argv = (const char **)fz_argv_from_wargv(argc, wargv);
	if (!argv)
		return EXIT_FAILURE;
	int ret = main(argc, argv);
	fz_free_argv(argc, argv);
	return ret;
}
#endif

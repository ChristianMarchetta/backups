package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cmar0027/backups/pkg/backups"
	"github.com/cmar0027/backups/pkg/retries"
	"github.com/lithammer/dedent"
)

func exit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		exit("Please provide a command name")
	}

	command := os.Args[1]

	switch command {
	case "restore":
		restore(os.Args[2:])
	case "backup":
		backup(os.Args[2:])
	default:
		exit("Unknown command: " + command + dedent.Dedent(`
		Usage: backups <command> [args]

		Avaliable commands:
			backup  	Backup files to S3
			restore 	Restore files from S3
		`))
	}
}

func registerRateLimitingArgs(set *flag.FlagSet) *retries.RateLimiterArgs {
	limiter := &retries.RateLimiterArgs{}

	set.IntVar(&limiter.RequestsPerMin, "rpm", 100, "The number of allowed requests per minute to the S3 API")
	set.IntVar(&limiter.MaxRetries, "max-retries", 10, "The number of allowed retries for S3 API calls. The retry time is computed as 2^retry * 100ms")

	return limiter
}

func registerS3Args(set *flag.FlagSet) *backups.S3Args {
	s3 := &backups.S3Args{}

	set.StringVar(&s3.Region, "s3-region", "", "The S3 region to use")
	set.StringVar(&s3.Bucket, "s3-bucket", "", "The S3 bucket name to use")
	set.StringVar(&s3.EndPoint, "s3-endpoint", "", "The S3 endpoint to use")
	set.StringVar(&s3.AccessKeyID, "s3-access-key-id", "", "The S3 access key id to use")
	set.StringVar(&s3.SecretAccessKey, "s3-secret-access-key", "", "The S3 secret access key to use. WARNING: This might be visible in your shell history or process list. Use the S3_SECRET_ACCESS_KEY environment variable instead.")

	return s3
}

func getS3ArgsFromEnv() *backups.S3Args {
	s3 := &backups.S3Args{}

	s3.Region = os.Getenv("S3_REGION")
	s3.Bucket = os.Getenv("S3_BUCKET")
	s3.EndPoint = os.Getenv("S3_ENDPOINT")
	s3.AccessKeyID = os.Getenv("S3_ACCESS_KEY_ID")
	s3.SecretAccessKey = os.Getenv("S3_SECRET_ACCESS_KEY")

	return s3
}

func coalesceS3Args(fromArgs *backups.S3Args, fromEnv *backups.S3Args) backups.S3Args {

	ifzero := func(src string, alternative string) string {
		if src == "" {
			return alternative
		}
		return src
	}

	return backups.S3Args{
		Region:          ifzero(fromEnv.Region, fromArgs.Region),
		Bucket:          ifzero(fromEnv.Bucket, fromArgs.Bucket),
		EndPoint:        ifzero(fromEnv.EndPoint, fromArgs.EndPoint),
		AccessKeyID:     ifzero(fromEnv.AccessKeyID, fromArgs.AccessKeyID),
		SecretAccessKey: ifzero(fromEnv.SecretAccessKey, fromArgs.SecretAccessKey),
	}
}

func restore(args []string) {
	if len(args) < 1 {
		exit("Please provide a backup file")
	}

	restore := flag.NewFlagSet("restore", flag.ExitOnError)
	force := restore.Bool("force", false, "if true, restore will overwrite existing files")
	concurrentTransfers := restore.Uint("concurrent", 10, "The number of allowed concurrent downloads")
	s3ArgsFromShell := registerS3Args(restore)
	rateLimitingArgs := registerRateLimitingArgs(restore)

	err := restore.Parse(args)
	if err != nil {
		exit(err.Error())
	}

	files := restore.Args()

	if len(files) < 1 {
		exit("No files to restore")
	}

	s3Args := coalesceS3Args(s3ArgsFromShell, getS3ArgsFromEnv())

	if *concurrentTransfers >= 10000 {
		exit("Too many concurrent downloads")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	transferrer := backups.NewS3Transferrer(s3Args, int(*concurrentTransfers), cancelFunc, *rateLimitingArgs, &backups.RestorerArgs{
		Force: *force,
	}, nil)

	for _, file := range files {
		parts := strings.Split(file, ":")
		if len(parts) != 2 {
			exit("Invalid <src>:<dest> pair: " + file)
		}
		src := strings.TrimSpace(parts[0])
		dest := strings.TrimSpace(parts[1])

		if src == "" || dest == "" {
			exit("Invalid <src>:<dest> pair: " + file)
		}

		transferrer.Restore(ctx, backups.FileTransfer{
			Remote: src,
			Local:  dest,
		})
	}

	transferrer.Wait()

	if transferrer.CtxErrCanceler.IsCanceled() {
		exit(transferrer.CtxErrCanceler.String())
	}
}

func backup(args []string) {
	if len(args) < 1 {
		exit("Please provide a backup file")
	}

	backup := flag.NewFlagSet("backup", flag.ExitOnError)

	concurrentTransfers := backup.Uint("concurrent", 10, "The number of allowed concurrent uploads")
	skipNotModiefiedFor := backup.Duration("skip-not-modified-for", 0, dedent.Dedent(`Duration string. If set, files that have not been modified for the duration period or more will be ignored.
		Value must be parsable by Golang's time.ParseDuration() function.
		"A duration string is a [...] signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '-1.5h' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'."
		Note, using this option will cause an additional S3 object stat call for each local file.
		`))
	rateLimitingArgs := registerRateLimitingArgs(backup)
	s3ArgsFromShell := registerS3Args(backup)

	err := backup.Parse(args)
	if err != nil {
		exit(err.Error())
	}

	files := backup.Args()

	if len(files) < 1 {
		exit("No files to backup")
	}

	s3Args := coalesceS3Args(s3ArgsFromShell, getS3ArgsFromEnv())

	if *concurrentTransfers >= 10000 {
		exit("Too many concurrent downloads")
	}

	if *skipNotModiefiedFor < time.Duration(0) {
		exit("-skip-not-modified-for must be a positive duration")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	transferrer := backups.NewS3Transferrer(s3Args, int(*concurrentTransfers), cancelFunc, *rateLimitingArgs, nil, &backups.BackupperArgs{
		SkipNotMofiedFor: *skipNotModiefiedFor,
	})

	for _, file := range files {
		parts := strings.Split(file, ":")
		if len(parts) != 2 {
			exit("Invalid <src>:<dest> pair: " + file)
		}
		src := strings.TrimSpace(parts[0])
		dest := strings.TrimSpace(parts[1])

		if src == "" || dest == "" {
			exit("Invalid <src>:<dest> pair: " + file)
		}

		transferrer.Backup(ctx, backups.FileTransfer{
			Remote: src,
			Local:  dest,
		})
	}

	transferrer.Wait()

	if transferrer.CtxErrCanceler.IsCanceled() {
		exit(transferrer.CtxErrCanceler.String())
	}
}

package common

import (
	"github.com/nuclio/logger"
	"github.com/nuclio/zap"
	"github.com/pkg/errors"
	"github.com/v3io/xcp/backends"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func NewLogger(level string) (logger.Logger, error) {
	var logLevel nucliozap.Level
	switch level {
	case "debug":
		logLevel = nucliozap.DebugLevel
	case "info":
		logLevel = nucliozap.InfoLevel
	case "warn":
		logLevel = nucliozap.WarnLevel
	case "error":
		logLevel = nucliozap.ErrorLevel
	default:
		logLevel = nucliozap.WarnLevel
	}

	log, err := nucliozap.NewNuclioZapCmd("v3ctl", logLevel)
	if err != nil {
		return nil, err
	}
	return log, nil
}

func UrlParse(fullpath string, forceDir bool) (*backends.PathParams, error) {
	if !strings.Contains(fullpath, "://") {
		params := &backends.PathParams{}
		err := backends.ParseFilename(fullpath, params, forceDir)
		return params, err
	}

	u, err := url.Parse(fullpath)
	if err != nil {
		return nil, err
	}

	pathParams := backends.PathParams{
		Kind: strings.ToLower(u.Scheme),
		Tag:  u.Fragment,
	}
	if strings.HasPrefix(u.Path, "/") {
		u.Path = u.Path[1:]
	}
	err = backends.ParseFilename(u.Path, &pathParams, forceDir)
	if err != nil {
		return nil, err
	}

	password, hasPassword := u.User.Password()
	if hasPassword && u.User.Username() == "" {
		pathParams.Token = password
	} else {
		pathParams.UserKey = u.User.Username()
		pathParams.Secret = password
	}

	switch pathParams.Kind {
	case "s3":
		// TODO: region url
		pathParams.Bucket = u.Host
	case "v3io":
		pathParams.Kind = "v3io"
		pathParams.Endpoint = u.Host
		pathParams.Bucket, pathParams.Path = backends.SplitPath(pathParams.Path)
	case "v3ios":
		pathParams.Secure = true
		pathParams.Kind = "v3io"
		pathParams.Endpoint = u.Host
		pathParams.Bucket, pathParams.Path = backends.SplitPath(pathParams.Path)
	case "http", "https":
		pathParams.Secure = pathParams.Kind == "https"
		pathParams.Kind = "s3"
		pathParams.Endpoint = u.Host
		pathParams.Bucket, pathParams.Path = backends.SplitPath(pathParams.Path)
	default:
		pathParams.Endpoint = u.Host
	}

	return &pathParams, nil
}

const (
	OneSecMs    = 1000
	OneMinuteMs = 60 * OneSecMs
	OneHourMs   = 60 * OneMinuteMs
	OneDayMs    = 24 * OneHourMs
	OneYearMs   = 365 * OneDayMs
)

// Convert a time string to a time object.
// The input time string can be of the format "now", "now-[0-9]+[mdh]" (for
// example, "now-2h"), "<Unix timestamp>", or "<RFC 3339 time>" (for example, "2018-09-26T14:10:20Z").
func String2Time(timeString string) (time.Time, error) {
	if timeString == "" {
		return time.Time{}, nil
	}

	if strings.HasPrefix(timeString, "now") {
		now := time.Now()
		if len(timeString) > 3 {
			sign := timeString[3:4]
			duration := timeString[4:]

			d, err := Str2duration(duration)
			if err != nil {
				return time.Time{}, errors.Wrap(err, "Could not parse the pattern following 'now-'.")
			}
			if sign == "-" {
				return now.Add(-d), nil
			} else if sign == "+" {
				return now.Add(d), nil
			} else {
				return time.Time{}, errors.Wrapf(err, "Unsupported time format:", timeString)
			}
		} else {
			return now, nil
		}
	}

	tint, err := strconv.Atoi(timeString)
	if err == nil {
		return time.Unix(int64(tint), 0), nil
	}

	t, err := time.Parse(time.RFC3339, timeString)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "Invalid time string - not an RFC 3339 time format.")
	}
	return t, nil
}

// Convert a "[0-9]+[mhd]" duration string (for example, "24h") to a
// Unix timestamp in milliseconds integer
func Str2duration(duration string) (time.Duration, error) {

	multiply := OneHourMs // 1 hour by default
	if len(duration) > 0 {
		last := duration[len(duration)-1:]
		if last == "s" || last == "m" || last == "h" || last == "d" || last == "y" {
			duration = duration[0 : len(duration)-1]
			switch last {
			case "s":
				multiply = OneSecMs
			case "m":
				multiply = OneMinuteMs
			case "h":
				multiply = OneHourMs
			case "d":
				multiply = OneDayMs
			case "y":
				multiply = OneYearMs
			}
		}
	}

	if duration == "" {
		return 0, nil
	}

	i, err := strconv.Atoi(duration)
	if err != nil {
		return 0, errors.Wrap(err,
			`Invalid duration. Accepted pattern: [0-9]+[mhd]. Examples: "30d" (30 days); "5m" (5 minutes).`)
	}
	if i < 0 {
		return 0, errors.Errorf("The specified duration (%s) is negative.", duration)
	}

	return time.Duration(time.Millisecond * time.Duration(multiply*i)), nil
}

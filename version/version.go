package version

import (
	"encoding/json"
	"fmt"
	"runtime"

	"golang.org/x/mod/semver"
)

// current PBM version
const version = "2.2.1"

var (
	platform  string
	gitCommit string
	gitBranch string
	buildTime string
)

type Info struct { //nolint:musttag
	Version   string
	Platform  string
	GitCommit string
	GitBranch string
	BuildTime string
	GoVersion string
}

const plain = `Version:   %s
Platform:  %s
GitCommit: %s
GitBranch: %s
BuildTime: %s
GoVersion: %s`

func Current() Info {
	v := Info{
		Version:   version,
		Platform:  platform,
		GitCommit: gitCommit,
		GitBranch: gitBranch,
		BuildTime: buildTime,
		GoVersion: runtime.Version(),
	}
	if v.Platform == "" {
		v.Platform = fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	}

	return v
}

func (i Info) String() string {
	return fmt.Sprintf(plain,
		i.Version,
		i.Platform,
		i.GitCommit,
		i.GitBranch,
		i.BuildTime,
		i.GoVersion,
	)
}

func (i Info) Short() string {
	return i.Version
}

func (i Info) All(format string) string {
	switch format {
	case "":
		return fmt.Sprintf(plain,
			i.Version,
			i.Platform,
			i.GitCommit,
			i.GitBranch,
			i.BuildTime,
			i.GoVersion,
		)
	case "json":
		v, _ := json.MarshalIndent(i, "", " ") //nolint:errchkjson
		return string(v)
	default:
		return fmt.Sprintf("%#v", i)
	}
}

// CompatibleWith checks if a given version is compatible the current one. It
// is not compatible if the current is crossed the breaking ponit
// (version >= breakingVersion) and the given isn't (v < breakingVersion)
func CompatibleWith(v string, breakingv []string) bool {
	return compatible(version, v, breakingv)
}

func compatible(v1, v2 string, breakingv []string) bool {
	if len(breakingv) == 0 {
		return true
	}

	v1 = majmin(v1)
	v2 = majmin(v2)

	c := semver.Compare(v2, v1)
	if c == 0 {
		return true
	}

	hV, lV := v1, v2
	if c == 1 {
		hV, lV = lV, hV
	}

	for i := len(breakingv) - 1; i >= 0; i-- {
		cb := majmin(breakingv[i])
		if semver.Compare(hV, cb) >= 0 {
			return semver.Compare(lV, cb) >= 0
		}
	}

	return true
}

func majmin(v string) string {
	if len(v) == 0 {
		return v
	}

	if v[0] != 'v' {
		v = "v" + v
	}

	return semver.MajorMinor(v)
}

func IsLegacyArchive(ver string) bool {
	return semver.Compare(majmin(ver), "v2.0") == -1
}

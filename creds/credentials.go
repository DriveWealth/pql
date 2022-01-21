package creds

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"os"
	"pql/ec2"
	"runtime"
)

// Retrieve(ctx context.Context) (Credentials, error)

type ChainedCredentialProvider struct {
	Providers []aws.CredentialsProvider
	curr      aws.CredentialsProvider
	callback  func(string)
}

func NewChainedCredentialProvider(providers ...aws.CredentialsProvider) *ChainedCredentialProvider {
	return NewChainedCredentialProviderWithRef(nil, providers...)
}

func NewChainedCredentialProviderWithRef(awsKeyId func(string), providers ...aws.CredentialsProvider) *ChainedCredentialProvider {
	return &ChainedCredentialProvider{
		Providers: providers,
		curr:      nil,
		callback:  awsKeyId,
	}
}

func (ccp *ChainedCredentialProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	var errs []error
	for _, p := range ccp.Providers {
		creds, err := p.Retrieve(ctx)
		if err == nil {
			ccp.curr = p
			if ccp.callback != nil {
				ccp.callback(creds.AccessKeyID)
			}
			return creds, nil
		}
		errs = append(errs, err)
	}
	ccp.curr = nil

	var err error
	err = errors.New("ErrNoValidProvidersFoundInChain")
	return aws.Credentials{}, err
}

func FileExists(fileName string) bool {
	info, err := os.Stat(fileName)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func GetEnvProfile() string {
	env := os.Getenv("AWS_PROFILE")
	if env == "" {
		return ""
	}
	if FileExists(env) {
		return env
	}
	return ""
}

func GetProfile() string {
	envProf := GetEnvProfile()
	if envProf != "" {
		return envProf
	}
	var homeDir string
	var profile string
	var err error
	switch runtime.GOOS {
	case "windows":
		homeDir = os.Getenv("USERPROFILE")
		if homeDir == "" {
			return ""
		}
		profile = homeDir + `\.aws\config`
		if FileExists(profile) {
			return profile
		}
		profile = homeDir + `\.aws\credentials`
		if FileExists(profile) {
			return profile
		}
		return ""
	case "linux", "darwin":
		homeDir, err = os.UserHomeDir()
		if err != nil || homeDir == "" {
			return ""
		}
		profile = homeDir + `/.aws/config`
		if FileExists(profile) {
			return profile
		}
		profile = homeDir + `/.aws/credentials`
		if FileExists(profile) {
			return profile
		}
		return ""
	}
	return ""
}

func sharedConfigProfile(profile string) aws.CredentialsProvider {
	cfg, _ := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile(profile))
	return cfg.Credentials
}

const (
	LOCAL_KEY    = "AWS_ID"
	LOCAL_SECRET = "AWS_SECRET"
)

func BuildLocalDBChainedCredentialProvider() *ChainedCredentialProvider {
	return NewChainedCredentialProvider(credentials.NewStaticCredentialsProvider(LOCAL_KEY, LOCAL_SECRET, ""))
}

func BuildChainedCredentialProvider(awsKey, awsSecret, awsProfile, localDbUrl string) *ChainedCredentialProvider {
	if localDbUrl != "" {
		return BuildLocalDBChainedCredentialProvider()
	}
	if awsProfile != "" {
		a := sharedConfigProfile(awsProfile)
		return NewChainedCredentialProvider(a)
	}

	providers := make([]aws.CredentialsProvider, 0, 3)
	if ec2.OnEC2() {
		providers = append(providers, ec2rolecreds.New())
	}
	if awsKey != "" && awsSecret != "" {
		providers = append(providers, credentials.NewStaticCredentialsProvider(awsKey, awsSecret, ""))
	}

	return NewChainedCredentialProvider(providers...)
}

func GetProfileCreds(profile string) ([]string, error) {
	rez := make([]string, 4)
	cfg, er1 := config.LoadDefaultConfig(
		context.Background(),
		config.WithSharedConfigProfile(profile),
	)
	if er1 != nil {
		return nil, er1
	}
	creds, er2 := cfg.Credentials.Retrieve(context.Background())
	if er2 != nil {
		return nil, er2
	}
	rez[0] = creds.Source
	rez[1] = creds.AccessKeyID
	rez[2] = creds.SecretAccessKey
	rez[3] = "us-east-1" //FIXME

	for _, src := range cfg.ConfigSources {
		if src != nil {
			sc, ok := src.(config.SharedConfig)
			if ok {
				if sc.Profile == profile {
					if sc.Region != "" {
						rez[3] = sc.Region
						break
					}
				}
			}
		}
	}
	return rez, nil
}

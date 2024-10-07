provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "sa-east-1"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  skip_region_validation      = true
  s3_use_path_style           = true

  endpoints {
    s3 = "http://localstack:4566"
  }
}

provider "aws" {
  alias                       = "localhost"
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "localhost"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  skip_region_validation      = true

  endpoints {
    dynamodb = "http://localstack:4566"
  }
}
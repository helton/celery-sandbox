resource "aws_s3_bucket" "data_bucket" {
  bucket = "data-bucket"
}

resource "aws_dynamodb_table" "task" {
  provider       = aws.localhost
  name           = "task"
  hash_key       = "id"
  billing_mode   = "PAY_PER_REQUEST"

  attribute {
    name = "id"
    type = "S"
  }
}

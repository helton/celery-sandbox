output "s3_bucket_name" {
  value = aws_s3_bucket.data_bucket.bucket
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.task.name
}

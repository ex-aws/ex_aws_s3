defmodule ExAws.S3.SelectObjectContentsTest do
  use ExUnit.Case, async: true

  alias ExAws.S3.SelectObjectContents

  describe "SelectObjectContents.build_payload/4" do
    test "default payload" do
      assert SelectObjectContents.build_payload(
               "select * from s3object",
               %{csv: %{}},
               %{csv: %{}},
               nil
             ) ==
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Expression>select * from s3object</Expression>\n  <ExpressionType>SQL</ExpressionType>\n  <InputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><FileHeaderInfo>USE</FileHeaderInfo><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </InputSerialization>\n  <OutputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </OutputSerialization>\n  \n</SelectObjectContentRequest>\n"
    end

    test "CSV input" do
      assert SelectObjectContents.build_payload(
               "select * from s3object",
               %{
                 csv: %{
                   file_header_info: "IGNORE",
                   record_delimiter: "\n",
                   field_delimiter: ",",
                   quote_character: "\"",
                   quote_escape_character: "\"",
                   comments: "#",
                   allow_quoted_record_delimiter: false
                 }
               },
               %{csv: %{}},
               nil
             ) ==
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Expression>select * from s3object</Expression>\n  <ExpressionType>SQL</ExpressionType>\n  <InputSerialization>\n  <CSV><AllowQuotedRecordDelimiter>FALSE</AllowQuotedRecordDelimiter><Comments>#</Comments><FieldDelimiter>,</FieldDelimiter><FileHeaderInfo>IGNORE</FileHeaderInfo><QuoteCharacter>\"</QuoteCharacter><QuoteEscapeCharacter>\"</QuoteEscapeCharacter><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </InputSerialization>\n  <OutputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </OutputSerialization>\n  \n</SelectObjectContentRequest>\n"
    end

    test "JSON Input" do
      assert SelectObjectContents.build_payload(
               "select * from s3object",
               %{
                 json: %{
                   type: :document,
                   record_delimiter: "\n"
                 }
               },
               %{csv: %{}},
               nil
             ) ==
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Expression>select * from s3object</Expression>\n  <ExpressionType>SQL</ExpressionType>\n  <InputSerialization>\n  <JSON><RecordDelimiter>\n</RecordDelimiter><Type>DOCUMENT</Type></JSON>\n  </InputSerialization>\n  <OutputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </OutputSerialization>\n  \n</SelectObjectContentRequest>\n"
    end

    test "Parquet Input" do
      assert SelectObjectContents.build_payload(
               "select * from s3object",
               %{
                 parquet: %{
                   record_delimiter: "\n"
                 }
               },
               %{csv: %{}},
               nil
             ) ==
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Expression>select * from s3object</Expression>\n  <ExpressionType>SQL</ExpressionType>\n  <InputSerialization>\n  <Parquet></Parquet>\n  </InputSerialization>\n  <OutputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </OutputSerialization>\n  \n</SelectObjectContentRequest>\n"
    end

    test "CSV Output" do
      assert SelectObjectContents.build_payload(
               "select * from s3object",
               %{csv: %{}},
               %{
                 csv: %{
                   record_delimiter: "\n",
                   field_delimiter: ",",
                   quote_character: "\"",
                   quote_escape_character: "\"",
                   quote_fields: :asneeded,
                   comments: "#"
                 }
               },
               nil
             ) ==
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Expression>select * from s3object</Expression>\n  <ExpressionType>SQL</ExpressionType>\n  <InputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><FileHeaderInfo>USE</FileHeaderInfo><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </InputSerialization>\n  <OutputSerialization>\n  <CSV><Comments>#</Comments><FieldDelimiter>,</FieldDelimiter><QuoteCharacter>\"</QuoteCharacter><QuoteEscapeCharacter>\"</QuoteEscapeCharacter><QuoteFields>ASNEEDED</QuoteFields><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </OutputSerialization>\n  \n</SelectObjectContentRequest>\n"
    end

    test "JSON Output" do
      assert SelectObjectContents.build_payload(
               "select * from s3object",
               %{csv: %{}},
               %{
                 json: %{
                   record_delimiter: "\n"
                 }
               },
               nil
             ) ==
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Expression>select * from s3object</Expression>\n  <ExpressionType>SQL</ExpressionType>\n  <InputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><FileHeaderInfo>USE</FileHeaderInfo><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </InputSerialization>\n  <OutputSerialization>\n  <JSON><RecordDelimiter>\n</RecordDelimiter></JSON>\n  </OutputSerialization>\n  \n</SelectObjectContentRequest>\n"
    end

    test "Scan Range" do
      assert SelectObjectContents.build_payload(
               "select * from s3object",
               %{csv: %{}},
               %{csv: %{}},
               %{start: 0, end: 100}
             ) ==
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<SelectObjectContentRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Expression>select * from s3object</Expression>\n  <ExpressionType>SQL</ExpressionType>\n  <InputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><FileHeaderInfo>USE</FileHeaderInfo><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </InputSerialization>\n  <OutputSerialization>\n  <CSV><FieldDelimiter>,</FieldDelimiter><RecordDelimiter>\n</RecordDelimiter></CSV>\n  </OutputSerialization>\n  <ScanRange><End>100</End><Start>0</Start></ScanRange>\n</SelectObjectContentRequest>\n"
    end
  end
end

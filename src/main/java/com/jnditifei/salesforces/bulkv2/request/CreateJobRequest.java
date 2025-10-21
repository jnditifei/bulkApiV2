package com.jnditifei.salesforces.bulkv2.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jnditifei.salesforces.bulkv2.type.ColumnDelimiterEnum;
import com.jnditifei.salesforces.bulkv2.type.ContentTypeEnum;
import com.jnditifei.salesforces.bulkv2.type.LineEndingEnum;
import com.jnditifei.salesforces.bulkv2.type.OperationEnum;


import java.io.File;

public class CreateJobRequest {

    private final ColumnDelimiterEnum columnDelimiter;

    private final ContentTypeEnum contentType;

    private final String externalIdFieldName;

    private final LineEndingEnum lineEnding;

    private final String object;

    private final OperationEnum operation;

    @JsonIgnore
    private final String content;

    @JsonIgnore
    private final File contentFile;

    public ColumnDelimiterEnum getColumnDelimiter() {
        return columnDelimiter;
    }

    public ContentTypeEnum getContentType() {
        return contentType;
    }

    public String getExternalIdFieldName() {
        return externalIdFieldName;
    }

    public LineEndingEnum getLineEnding() {
        return lineEnding;
    }

    public String getObject() {
        return object;
    }

    public OperationEnum getOperation() {
        return operation;
    }

    public String getContent() {
        return content;
    }

    public File getContentFile() {
        return contentFile;
    }

    private CreateJobRequest(Builder builder) {
        this.columnDelimiter = builder.columnDelimiter;
        this.contentType = builder.contentType;
        this.externalIdFieldName = builder.externalIdFieldName;
        this.lineEnding = builder.lineEnding;
        this.object = builder.object;
        this.operation = builder.operation;
        this.content = builder.content;
        this.contentFile = builder.contentFile;
    }

    public static class Builder {

        private String object;

        private OperationEnum operation;

        private ColumnDelimiterEnum columnDelimiter;

        private ContentTypeEnum contentType;

        private String externalIdFieldName;

        private LineEndingEnum lineEnding;

        private String content;

        private File contentFile;

        public Builder(String object, OperationEnum operation, String columnDelimiter, String lineEnding) {
            this.object = object;
            this.operation = operation;
            this.contentType = ContentTypeEnum.CSV;
            this.columnDelimiter = ColumnDelimiterEnum.valueOf(columnDelimiter);
            this.lineEnding = LineEndingEnum.valueOf(lineEnding);
        }

        public Builder withExternalIdFieldName(String externalIdFieldName) {
            this.externalIdFieldName = externalIdFieldName;
            return this;
        }

        public Builder withContent(String content) {
            this.content = content;
            return this;
        }

        public Builder withContent(File file) {
            this.contentFile = contentFile;
            return this;
        }

        public CreateJobRequest build() {
            return new CreateJobRequest(this);
        }
    }
}

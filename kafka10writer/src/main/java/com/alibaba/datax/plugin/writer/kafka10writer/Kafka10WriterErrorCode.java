package com.alibaba.datax.plugin.writer.kafka10writer;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author 000623
 * @since 2019/4/9 12:07
 */
public enum  Kafka10WriterErrorCode implements ErrorCode {

    REQUIRED_VALUE("Kafka10Writer-01", "您缺失了必须填写的参数值."),
    ;

    private final String code;
    private final String description;

    private Kafka10WriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}

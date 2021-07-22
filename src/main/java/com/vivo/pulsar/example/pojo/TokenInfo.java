package com.vivo.pulsar.example.pojo;

import io.jsonwebtoken.SignatureAlgorithm;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author sikang.liu
 * @date 2021-03-19 14:12
 * @description
 */
@Slf4j
@Data
public class TokenInfo {

    SignatureAlgorithm algorithm = SignatureAlgorithm.RS256;

    private String subject;

    private String expiryTime;

    private String secretKey;

    private String privateKey;
}

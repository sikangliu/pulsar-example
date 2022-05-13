package com.vivo.pulsar.example.utils;

import com.vivo.pulsar.example.pojo.TokenInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.common.util.RelativeTimeUtil;

import java.security.Key;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author sikang.liu
 * @date 2021-03-19 14:13
 * @description
 */
@Slf4j
public class TokensCliUtils {

    public static String CreateToken(TokenInfo tokenInfo) {
        String secretKey = tokenInfo.getSecretKey();
        String privateKey = tokenInfo.getPrivateKey();

        String token = null;
        try {
            if (secretKey == null && privateKey == null) {
                log.error("Either --secret-key or --private-key needs to be passed for signing a token");
                throw new Exception("需要传递--secret-key或--private-key来对令牌进行签名");
            } else if (secretKey != null && privateKey != null) {
                log.error("Only one of --secret-key and --private-key needs to be passed for signing a token");
                throw new Exception("仅需传递--secret-key和--private-key中的一个即可对令牌进行签名");
            }

            Key signingKey;

            if (privateKey != null) {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(privateKey);
                signingKey = AuthTokenUtils.decodePrivateKey(encodedKey, tokenInfo.getAlgorithm());
            } else {
                byte[] encodedKey = AuthTokenUtils.readKeyFromUrl(secretKey);
                signingKey = AuthTokenUtils.decodeSecretKey(encodedKey);
            }

            Optional<Date> optExpiryTime = Optional.empty();
            if (tokenInfo.getExpiryTime() != null) {
                long relativeTimeMillis = TimeUnit.SECONDS
                        .toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(tokenInfo.getExpiryTime()));
                optExpiryTime = Optional.of(new Date(System.currentTimeMillis() + relativeTimeMillis));
            }

            token = AuthTokenUtils.createToken(signingKey, tokenInfo.getSubject(), optExpiryTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return token;
    }

    public static void main(String[] args) {
        //String url = TokensCliUtils.class.getResource("/my-secret.key").getPath().substring(1);
        String SecretKey = "K/rRw0hzNrkdnlG1gFYcOYFf7yQDmdigRRQ9ueqopSs=";

        TokenInfo tokenInfo = new TokenInfo();
        tokenInfo.setSubject("lsk");
        tokenInfo.setSecretKey(SecretKey);
        String token = TokensCliUtils.CreateToken(tokenInfo);
        System.out.println(token);
        tokenInfo.setSubject("lsk1");
        tokenInfo.setSecretKey(SecretKey);
        String token1 = TokensCliUtils.CreateToken(tokenInfo);
        System.out.println(token1);

    }

}
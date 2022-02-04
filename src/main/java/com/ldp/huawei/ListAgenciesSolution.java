package com.ldp.huawei;

import com.huaweicloud.sdk.core.auth.ICredential;
import com.huaweicloud.sdk.core.auth.GlobalCredentials;
import com.huaweicloud.sdk.core.exception.ConnectionException;
import com.huaweicloud.sdk.core.exception.RequestTimeoutException;
import com.huaweicloud.sdk.core.exception.ServiceResponseException;
import com.huaweicloud.sdk.iam.v3.region.IamRegion;
import com.huaweicloud.sdk.iam.v3.*;
import com.huaweicloud.sdk.iam.v3.model.*;


public class ListAgenciesSolution {

    public static void main(String[] args) {
        String ak = "4L3UFLXQAU8EAGWKCEZG";
        String sk = "uj4KLsXN0JV2vUgAtsynhWQKngMyi58ntqWqdA0x";

        ICredential auth = new GlobalCredentials()
                .withAk(ak)
                .withSk(sk);

        IamClient client = IamClient.newBuilder()
                .withCredential(auth)
                .withRegion(IamRegion.valueOf("cn-north-4"))
                .build();
        ListAgenciesRequest request = new ListAgenciesRequest();
        try {
            ListAgenciesResponse response = client.listAgencies(request);
            System.out.println(response.toString());
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (RequestTimeoutException e) {
            e.printStackTrace();
        } catch (ServiceResponseException e) {
            e.printStackTrace();
            System.out.println(e.getHttpStatusCode());
            System.out.println(e.getErrorCode());
            System.out.println(e.getErrorMsg());
        }
    }
}
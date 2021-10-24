package com.ycu.tang.msbplatform.batch.test;


import com.ycu.tang.msbplatform.batch.thrift.*;

public class Data {
    public static Pedigree makePedigree(int timeSecs) {
        return new Pedigree(timeSecs);

    }

    public static com.ycu.tang.msbplatform.batch.thrift.Data makePageview(int userid, String url, int timeSecs) {
        return new com.ycu.tang.msbplatform.batch.thrift.Data(makePedigree(timeSecs),
                DataUnit.page_view(
                        new PageViewEdge(
                                PersonID.user_id(userid),
                                PageID.url(url),
                                1
                        )));
    }

    public static com.ycu.tang.msbplatform.batch.thrift.Data makeEquiv(int user1, int user2) {
        return new com.ycu.tang.msbplatform.batch.thrift.Data(makePedigree(1000),
                DataUnit.equiv(
                        new EquivEdge(
                                PersonID.user_id(user1),
                                PersonID.user_id(user2)
                        )));
    }


}

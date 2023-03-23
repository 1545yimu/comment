package com.hmdp.utils;

import org.springframework.web.servlet.HandlerInterceptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LoginInterceptor implements HandlerInterceptor {

    //preHandle：在controller之前执行
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        if(UserHolder.getUser() == null){
            // 没有需要拦截，设置状态码401
            response.setStatus(401);
            return false;
        }

        //6.放行
        return true;
    }

}

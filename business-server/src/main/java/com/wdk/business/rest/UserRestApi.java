package com.wdk.business.rest;

import com.wdk.business.model.domain.User;
import com.wdk.business.model.request.LoginUserRequest;
import com.wdk.business.model.request.RegisterUserRequest;
import com.wdk.business.service.UserService;
import com.wdk.business.utils.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@RequestMapping("/rest/users")
@Controller
public class UserRestApi {

    private static final Logger logger = LoggerFactory.getLogger(UserRestApi.class);

    @Autowired
    private UserService userService;

    @RequestMapping(value = "/login", produces = "application/json", method = RequestMethod.GET )
    @ResponseBody
    public Model login(@RequestParam("username") String username, @RequestParam("password") String password, Model model) {
        User user  =userService.loginUser(new LoginUserRequest(username,password));

        logger.info(Constant.USER_LOGIN_PREFIX + ":" + user.getUserId() +"|"+ "login" +"|"+ System.currentTimeMillis());

        model.addAttribute("success",user != null);
        model.addAttribute("user",user);
        return model;
    }

    @RequestMapping(value = "/register", produces = "application/json", method = RequestMethod.GET)
    @ResponseBody
    public Model addUser(@RequestParam("username") String username,@RequestParam("password") String password,Model model) {
        if(userService.checkUserExist(username)){
            model.addAttribute("success",false);
            model.addAttribute("message"," 用户名已经被注册！");
            return model;
        }
        model.addAttribute("success", userService.registerUser(new RegisterUserRequest(username,password)));
        return model;
    }
}

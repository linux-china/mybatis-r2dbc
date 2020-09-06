package org.apache.ibatis.r2dbc.demo;

import java.util.Date;

/**
 * User
 *
 * @author linux_china
 */
public class User {
    private Integer id;
    private String nick;
    private Date createdAt;

    public User() {
    }


    public User(Integer id, String nick) {
        this.id = id;
        this.nick = nick;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getNick() {
        return nick;
    }

    public void setNick(String nick) {
        this.nick = nick;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
}

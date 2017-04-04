package com.hadoop.entry;

import java.util.List;

import com.hadoop.util.DateUtil;

/**
 * 项目定义。
 * 
 *
 */
public class Project {
    
    /** 打印输出 */
    private static final String DESC = "[%s][%s][%9d]Project: %s, Total: %d, Success: %d, Failure: %d, Submitted: %d. ";

    /**
     * 项目名称
     */
    private String name;
    
    /**
     * 项目中的任务定义
     */
    private List<JobDefine> jobs;
    
    /**
     * 成功任务个数
     */
    private int countSuccess;
    
    /**
     * 异步提交的任务个数
     */
    private int countSubmitted;
    
    /**
     * 总任务数（包含异步提交的任务数）
     */
    private int total;
    
    /**
     * 整个项目的总耗时
     */
    private long spend;
    
    public Project(String name, List<JobDefine> jobs) {
        this.name = name;
        this.jobs = jobs;
    }

    public int getCountSuccess() {
        return countSuccess;
    }

    public void addCountSuccess() {
        this.countSuccess += 1;
    }

    public int getCountFailure() {
        return total - countSuccess;
    }

    public int getCountSubmitted() {
        return countSubmitted;
    }

    public void addCountSubmitted() {
        this.countSubmitted += 1;
    }

    public int getTotal() {
        return total;
    }

    public void addTotal() {
        this.total += 1;
    }
    
    public void addSpend(long value) {
        this.spend += value;
    }
    
    public long getSpend() {
        return this.spend;
    }

    public String getName() {
        return name;
    }

    public List<JobDefine> getJobs() {
        return jobs;
    }
    
    @Override
    public String toString() {
        return String.format(DESC, DateUtil.currentDate(), (getCountFailure() == 0 ? "S" : "F"), this.getSpend(),
                this.name, this.total, this.countSuccess, this.getCountFailure(), this.countSubmitted);
    }

    public void print() {
        System.out.println(toString());
    }
    
}

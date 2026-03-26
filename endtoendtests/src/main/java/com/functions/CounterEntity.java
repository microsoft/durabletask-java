package com.functions;

import com.microsoft.durabletask.AbstractTaskEntity;
import com.microsoft.durabletask.TaskEntityOperation;

/**
 * A simple counter entity for e2e testing.
 */
public class CounterEntity extends AbstractTaskEntity<Integer> {

    public int add(int input) {
        this.state += input;
        return this.state;
    }

    public int get() {
        return this.state;
    }

    public void reset() {
        this.state = 0;
    }

    @Override
    protected Integer initializeState(TaskEntityOperation operation) {
        return 0;
    }

    @Override
    protected Class<Integer> getStateType() {
        return Integer.class;
    }
}

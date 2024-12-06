//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <algorithm>
#include <queue>
static bool migrating = false;
static unsigned active_machines = 16;
static unsigned VM_OVERHEAD = 8;
static unsigned tasks_done = 0; 
static unsigned sla_violations = 0;
static int reverse_limit = 0;
    
struct TaskPriorityComparator {
    bool operator()(TaskId_t a, TaskId_t b) {
        SLAType_t aReqSLA = RequiredSLA(a);
        SLAType_t bReqSLA = RequiredSLA(b);
        Time_t aTargetCompletion = GetTaskInfo(a).target_completion;
        Time_t bTargetCompletion = GetTaskInfo(b).target_completion;

        // first, we prioritize the SLA 
        // SLA0 highest priority, SLA3 is lowest
        if (aReqSLA != bReqSLA) {
            return aReqSLA < bReqSLA;
        }

        // if equal SLA, do the one that needs to be done first
        return aTargetCompletion > bTargetCompletion;
    }
};

priority_queue<int, vector<int>, TaskPriorityComparator> task_queue;


CPUPerformance_t mostEfficientPState(MachineId_t machine) {
    MachineInfo_t machineInfo = Machine_GetInfo(machine);
    unsigned numPStates = machineInfo.p_states.size();
    CPUPerformance_t mostEfficient = machineInfo.p_state;
    float bestEfficiency = 0;
    for (unsigned x = 0; x < numPStates; x++) {
        unsigned performance = machineInfo.performance[x];
        unsigned powerConsumption = machineInfo.p_states[x];
        float currEfficiency = (float)(performance) / (float)(powerConsumption);
        if (currEfficiency > bestEfficiency) {
            bestEfficiency = currEfficiency;
            mostEfficient = CPUPerformance_t(x);
        }
    }
    return mostEfficient;
}

float scoreEfficiency(MachineId_t machine) {
    MachineInfo_t machineInfo = Machine_GetInfo(machine);
    CPUPerformance_t bestState = CPUPerformance_t(0);//mostEfficientPState(machine);
    unsigned performance = machineInfo.performance[bestState];
    unsigned powerConsumption = machineInfo.p_states[bestState];
    return (float)(performance) / (float)(powerConsumption);
}

bool compareEnergyEfficiency(MachineId_t a, MachineId_t b) {
    return scoreEfficiency(a) > scoreEfficiency(b);  //  Sort in descending order
}

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    active_machines = Machine_GetTotal();


    for(unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
        pendingMachineStates[MachineId_t(i)] = S0;
    }    


    std::sort(machines.begin(), machines.end(), compareEnergyEfficiency);
    for(auto machine: machines) {
        cout << "efficiency for " << machine << " is " << scoreEfficiency(machine)  << endl;
    }
 }

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}


void Scheduler::handleQueue() {
    if (task_queue.empty()) {
        return; // exit if nothing to do
    }
    TaskId_t task_id = task_queue.top();

    VMType_t  reqVM = RequiredVMType(task_id);
    CPUType_t reqCPU = RequiredCPUType(task_id);
    unsigned reqMemory = GetTaskMemory(task_id);
    SLAType_t reqSLA = RequiredSLA(task_id);

    Priority_t priority = MID_PRIORITY;
    if (reqSLA == SLA0) {
        priority = HIGH_PRIORITY;
    } else if (reqSLA == SLA3) {
        priority = LOW_PRIORITY;
    }


    // iterate through machines from most efficient to least efficient
    // and find one to allocate tasks to 
    for (MachineId_t machine : machines) {
        MachineInfo_t machineInfo = Machine_GetInfo(machine);

        unsigned memRemaining = machineInfo.memory_size - machineInfo.memory_used;
        if (machineInfo.cpu != reqCPU || (int)memRemaining - (int)reqMemory - (int)VM_OVERHEAD < 0 || (float)machineInfo.active_vms > (float)(machineInfo.num_cpus) * (1.0)) {
            continue;
        }

        // special case error: machine sleeping when it's needed
        if (pendingMachineStates[machine] > S0 || machineInfo.s_state > S0) {
            // re-enable machine
            Machine_SetState(machine, S0);
            pendingMachineStates[machine] = S0;
            // cout << "restarting machine " << machine << endl;
            reverse_limit -= 10; // prevent any more machines from being powered down
            return;
        }
        
        // create VM for task and add task
        VMId_t newVM = VM_Create(reqVM, reqCPU);
        VM_Attach(newVM, machine);
        vms.push_back(newVM);


        VM_AddTask(newVM, task_id, priority);
        task_queue.pop();


        return;
    }

    // oh no! no servers can handle task!! we have to ensure all
    // servers are ramped back up
    for (auto machine: machines) {
        MachineInfo_t machineInfo = Machine_GetInfo(machine);
        if (pendingMachineStates[machine] > S0 || machineInfo.s_state > S0) {
            Machine_SetState(machine, S0);
            pendingMachineStates[machine] = S0;
        }
        if (machineInfo.p_state > P0)
            Machine_SetCorePerformance(machine, 0, CPUPerformance_t(0));
    }
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // add the new task to queue
    task_queue.push(task_id);
    handleQueue();
}

MachineState_t getNextState(MachineState_t currentState) {
    if (currentState < S5) {
        return static_cast<MachineState_t>(currentState + 1);
    } else {
        return S5;
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    float taskPercentage = ((float)tasks_done / (float)GetNumTasks()) * 100;
    

    // periodic check for broken machines
    for (auto machine: machines) {
        MachineInfo_t machineInfo = Machine_GetInfo(machine);
        if(machineInfo.active_tasks > 0 && (machineInfo.s_state > S0 || pendingMachineStates[machine] > S0) ) {
            cout << "machine off with tasks!!" << endl;
            Machine_SetState(machine, S0);
            pendingMachineStates[machine] = S0;
            reverse_limit = -1000;
        }
    }
                
    // only allow more machine power-downs if
    //  - at least one machine will be running after
    //  - 10% of tasks have been done (stops it from getting ahead of itself)
    if (reverse_limit + 1 < (int)(machines.size()) && taskPercentage >= 10) {
        reverse_limit++;
    }

    // if we violate an SLA, don't care abt efficiency anymore
    if (sla_violations > 0) {
        for (auto machine: machines) {
            MachineInfo_t machineInfo = Machine_GetInfo(machine);
            if (pendingMachineStates[machine] > S0 || machineInfo.s_state > S0) {
                Machine_SetState(machine, S0);
                pendingMachineStates[machine] = S0;
            }
            if (machineInfo.p_state > P0)
                Machine_SetCorePerformance(machine, 0, CPUPerformance_t(0));
        }
    }

    int count_backwards = 0;
    for (vector<MachineId_t>::reverse_iterator riter = machines.rbegin();
        riter != machines.rend(); ++riter) 
    { 
        count_backwards += 1;

        // hit limit on machines allowed to be turned off
        if (count_backwards >= reverse_limit || (float)(sla_violations) > (float)(GetNumTasks()) * (0.05)) {
            break;
        }

        MachineInfo_t mInfo = Machine_GetInfo(*riter);
        auto nextState = getNextState(mInfo.s_state);

        if(mInfo.active_tasks == 0 && task_queue.empty() && nextState != pendingMachineStates[*riter]) {
            Machine_SetState(*riter, nextState);
            pendingMachineStates[*riter] = nextState; 
        }
    } 


    // for(auto machine: machines) {
    //     MachineInfo_t mInfo = Machine_GetInfo(machine);
    //     cout << machine << " has " << mInfo.active_tasks << " tasks and " << mInfo.active_vms << " vms | S" << mInfo.s_state <<  " P" << mInfo.p_state << endl;
    // }
    cout << "--------" << endl;


    // repeatedly do task on queue (as long as it's actually dequeueing stuff)
    unsigned preHandleQueueSize;
    do {
        preHandleQueueSize = task_queue.size();
        handleQueue();
    } while(preHandleQueueSize > task_queue.size());


    cout << taskPercentage << "\% tasks complete at time " << now << endl;
    cout << task_queue.size() << " tasks in queue | " << sla_violations << " violations " << endl;
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
    tasks_done += 1;

    // shut down all inactive vms, delete em
    for (auto it = vms.begin(); it != vms.end(); ) {
        VMInfo_t vmInfo = VM_GetInfo(*it);
        if (vmInfo.active_tasks.size() == 0) {
            VM_Shutdown(*it);
            it = vms.erase(it); 
        } else {
            it++; 
        }
    }

}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    // static unsigned counts = 0;
    // counts++;
    // if(counts == 10) {
    //     migrating = true;
    //     VM_Migrate(1, 9);
    // }
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    // cout << "SLA WARN AT " << time << " FOR TASK " << task_id << endl; 
    sla_violations += 1;
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}

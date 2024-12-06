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
static int run_shrink_cooldown = 0;    

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

    unsigned counter = 0;
    for(unsigned i = 0; i < active_machines; i++) {
        counter += 1;
        if (counter == 1) {
            machines_running.push_back(MachineId_t(i));
        } else if (counter == 2) {
            machines_intermediate.push_back(MachineId_t(i));
        } else {
            counter = 0;
        }
        pendingMachineStates[MachineId_t(i)] = S0;
    }    
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}

void Scheduler::autoRescale() {
    return;
    // 3 possible cases

    // running too big (shrink it)
    unsigned numRunning = machines_running.size();
    if (numRunning > 2 && run_shrink_cooldown >= 10) {
        auto lastMachineInfo = Machine_GetInfo(machines_running[machines_running.size() - 2]);
        if (lastMachineInfo.active_tasks != 0) {
            return; // dont turn any more off if we can't
        } 

        unsigned numToShrink = numRunning / 10;
        unsigned i = 0;

        for (auto it = machines_running.begin(); it != machines_running.end(); ) {
            if (i == numToShrink) {
                break;
            }

            if (Machine_GetInfo(*it).active_tasks > 0) {
                it++; 
            }

            Machine_SetState(*it, S3);
            pendingMachineStates[*it] = S3;
            machines_intermediate.push_back(*it);

            it = machines_intermediate.erase(it); 
            i++;
        }
        run_shrink_cooldown = 0;
    }

    // intermed too big (turn some off)
    // unsigned numIntermed = machines_intermediate.size();
    // if (numIntermed > 2 && run_shrink_cooldown >= 10) {
    //     auto lastMachineInfo = Machine_GetInfo(machines_running[machines_running.size() - 2]);
    //     if (lastMachineInfo.active_tasks != 0) {
    //         return; // dont turn any more off if we can't
    //     } 

    //     unsigned numToShrink = numIntermed / 10;
    //     unsigned i = 0;
    //     for (auto it = machines_intermediate.begin(); it != machines_intermediate.end(); ) {
    //         if (i == numToShrink) {
    //             break;
    //         }

    //         Machine_SetState(*it, S5);
    //         pendingMachineStates[*it] = S5;
    //         machines_off.push_back(*it);

    //         it = machines_intermediate.erase(it); 
    //         i++;
    //     }
    //     run_shrink_cooldown = 0;
    // }

}

void Scheduler::scaleupRunning() {
    unsigned numIntermediate = machines_intermediate.size();

    unsigned intermed_to_on  = max((numIntermediate / 2), numIntermediate);
    

    // turn running some intermediate machines
    unsigned i = 0;
    for (auto it = machines_intermediate.begin(); it != machines_intermediate.end(); ) {
        if (i == intermed_to_on) {
            break;
        }
        Machine_SetState(*it, S0);
        pendingMachineStates[*it] = S0;
        machines_running.push_back(*it);
        it = machines_intermediate.erase(it); 
        i++;
    }

    run_shrink_cooldown = -100;
}

void Scheduler::handleQueue() {
    if (task_queue.empty()) {
        return;
    }

    TaskId_t task_id = task_queue.top();

    VMType_t  reqVM = RequiredVMType(task_id);
    bool reqGPU = IsTaskGPUCapable(task_id);
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
    MachineId_t lastMachine = machines_running[machines_running.size() -1];
    for (MachineId_t machine : machines_running) {
        MachineInfo_t machineInfo = Machine_GetInfo(machine);

        unsigned memRemaining = machineInfo.memory_size - machineInfo.memory_used;
        if (machineInfo.cpu != reqCPU || (int)memRemaining - (int)reqMemory - (int)VM_OVERHEAD < 0 || machineInfo.active_vms > machineInfo.num_cpus) {
            continue;
        }

        if (machine == lastMachine) {
            scaleupRunning();
        }
        
        // create VM for task and add task
        VMId_t newVM = VM_Create(reqVM, reqCPU);
        VM_Attach(newVM, machine);
        vms.push_back(newVM);
        VM_AddTask(newVM, task_id, priority);
        task_queue.pop();

        return;
    }
    
    // running machines over allocated
    scaleupRunning();
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // add the new task to queue
    task_queue.push(task_id);
    handleQueue();
}

MachineState_t getNextState(MachineState_t currentState) {
    if (currentState < S4) {
        return static_cast<MachineState_t>(currentState + 1);
    } else {
        return S4;
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    float taskPercentage = ((float)tasks_done / (float)GetNumTasks()) * 100;
    run_shrink_cooldown += 1;
    autoRescale();

                
    // only allow more machine power-downs if
    //  - at least one machine will be running after
    //  - 10% of tasks have been done (stops it from getting ahead of itself)




    for(auto machine: machines_intermediate) {
        MachineInfo_t mInfo = Machine_GetInfo(machine);
        cout << machine << " has " << mInfo.active_tasks << " tasks and " << mInfo.active_vms << " vms | S" << mInfo.s_state <<  " P" << mInfo.p_state << endl;
    }
    cout << "--------" << endl;


    // repeatedly do task on queue (as long as it's actually dequeueing stuff)
    unsigned preHandleQueueSize;// = task_queue.size();
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

// unsigned Scheduler::GetActiveMachinesOfCPUType(CPUType_t type) {
//     unsigned active_count = 0;
//     for (auto machine: machines) {
//         auto mInfo = Machine_GetInfo(machine);
//         if(mInfo.s_state == S0 && pendingMachineStates[machine] == S0 && mInfo.cpu == type) {
//             active_count++;
//         }
//     }
//     return active_count;
// }

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
    tasks_done += 1;

    // shut down all inactive vms
    for (auto it = vms.begin(); it != vms.end(); ) {
        VMInfo_t vmInfo = VM_GetInfo(*it);
        if (vmInfo.active_tasks.size() == 0) {
            VM_Shutdown(*it);
            it = vms.erase(it); 
        } else {
            it++; 
        }
    }

    autoRescale();
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

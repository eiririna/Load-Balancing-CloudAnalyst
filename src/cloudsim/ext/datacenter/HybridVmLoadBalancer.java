package cloudsim.ext.datacenter;

import cloudsim.ext.event.CloudSimEvent;
import cloudsim.ext.event.CloudSimEventListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HybridVmLoadBalancer extends VmLoadBalancer implements CloudSimEventListener {
    private Map<Integer, VirtualMachineState> vmStatesList;
    private Map<Integer, Integer> currentAllocationCounts;

    public HybridVmLoadBalancer(DatacenterController dcb) {
        dcb.addCloudSimEventListener(this);
        this.vmStatesList = dcb.getVmStatesList();
        this.currentAllocationCounts = Collections.synchronizedMap(new HashMap<>());
    }

    public int getNextAvailableVm() {
        int vmId = -1;
        // Step 5: Check if hashmap list size < VM state list size
        if (this.currentAllocationCounts.size() < this.vmStatesList.size()) {
            Iterator<Integer> vmIterator = this.vmStatesList.keySet().iterator();
            while (vmIterator.hasNext()) {
                int availableVmId = vmIterator.next();
                if (!this.currentAllocationCounts.containsKey(availableVmId)) {
                    vmId = availableVmId;
                    break;
                }
            }
        } else {
            // Step 5: Else, wait for the VM to get free
            // Implement throttled algorithm behavior here
            Iterator<Integer> itr = this.vmStatesList.keySet().iterator();
            while (itr.hasNext()) {
                int temp = itr.next();
                VirtualMachineState state = this.vmStatesList.get(temp);
                if (state.equals(VirtualMachineState.AVAILABLE)) {
                    vmId = temp;
                    break;
                }
            }
        }
        allocatedVm(vmId);
        return vmId;
    }

    public void cloudSimEventFired(CloudSimEvent e) {
        int vmId;
        if (e.getId() == 3002) {
            vmId = (Integer) e.getParameter("vm_id");
            // Step 7: Update the status of VM in VMs state list and hashmap list
            updateVmStatus(vmId, true);
        } else if (e.getId() == 3003) {
            vmId = (Integer) e.getParameter("vm_id");
            // Step 7: Update the status of VM in VMs state list and hashmap list
            updateVmStatus(vmId, false);
        }
    }

    // Utility method to update VM status
    private void updateVmStatus(int vmId, boolean isBusy) {
        if (isBusy) {
            this.vmStatesList.put(vmId, VirtualMachineState.BUSY);
            Integer currCount = this.currentAllocationCounts.getOrDefault(vmId, 0);
            this.currentAllocationCounts.put(vmId, currCount + 1);
        } else {
            this.vmStatesList.put(vmId, VirtualMachineState.AVAILABLE);
            Integer currCount = this.currentAllocationCounts.get(vmId);
            if (currCount != null && currCount > 0) {
                this.currentAllocationCounts.put(vmId, currCount - 1);
            }
        }
    }
}

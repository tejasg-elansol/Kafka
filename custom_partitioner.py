from kafka.partitioner.default import DefaultPartitioner

def custom_safety_partitioner(key, all_partitions, available_partitions):
    """
    Custom partitioner for safety alerts.
    Routes messages based on camera_id priority.
    
    Since we have 1 partition per topic, all messages go to partition 0.
    But this structure allows easy scaling to multiple partitions later.
    """
    if key is None:
        return available_partitions[0] if available_partitions else all_partitions[0]
    
    # Extract camera ID from key
    try:
        camera_id = key.decode('utf-8') if isinstance(key, bytes) else str(key)
        
        # Priority routing (can be customized)
        # For now, with 1 partition, all go to partition 0
        if len(all_partitions) == 1:
            return all_partitions[0]
        
        # If you scale to multiple partitions, use this logic
        partition_index = hash(camera_id) % len(all_partitions)
        return all_partitions[partition_index]
    
    except Exception as e:
        print(f"Partitioner error: {e}")
        return all_partitions[0] if all_partitions else available_partitions[0]
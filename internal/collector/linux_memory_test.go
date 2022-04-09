package collector

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestMeminfoCollector_Update(t *testing.T) {
	var input = pipelineInput{
		required: []string{
			"node_memory_MemTotal", "node_memory_MemFree", "node_memory_MemAvailable", "node_memory_MemUsed",
			"node_memory_Buffers", "node_memory_Cached", "node_memory_SwapCached",
			"node_memory_Active", "node_memory_Inactive", "node_memory_Active_anon",
			"node_memory_Inactive_anon", "node_memory_Active_file", "node_memory_Inactive_file",
			"node_memory_SwapTotal", "node_memory_SwapFree", "node_memory_SwapUsed",
			"node_memory_Dirty", "node_memory_Writeback", "node_memory_AnonPages", "node_memory_Mapped",
			"node_memory_Shmem", "node_memory_PageTables", "node_memory_HugePages_Total",
			"node_memory_HugePages_Free", "node_memory_HugePages_Rsvd", "node_memory_HugePages_Surp",
			"node_memory_Hugepagesize",
			// vmstat
			"node_vmstat_nr_anon_pages", "node_vmstat_nr_mapped", "node_vmstat_nr_dirty", "node_vmstat_nr_writeback",
			"node_vmstat_pgpgin", "node_vmstat_pgpgout", "node_vmstat_pswpin", "node_vmstat_pswpout",
		},
		optional: []string{
			// meminfo
			"node_memory_Bounce", "node_memory_FilePmdMapped", "node_memory_CmaFree", "node_memory_ShmemHugePages",
			"node_memory_KReclaimable", "node_memory_CommitLimit", "node_memory_Slab", "node_memory_AnonHugePages",
			"node_memory_FileHugePages", "node_memory_DirectMap4k", "node_memory_VmallocTotal", "node_memory_SReclaimable",
			"node_memory_VmallocUsed", "node_memory_DirectMap1G", "node_memory_Committed_AS", "node_memory_Unevictable",
			"node_memory_WritebackTmp", "node_memory_NFS_Unstable", "node_memory_DirectMap2M", "node_memory_Hugetlb",
			"node_memory_CmaTotal", "node_memory_Mlocked", "node_memory_ShmemPmdMapped", "node_memory_SUnreclaim",
			"node_memory_KernelStack", "node_memory_VmallocChunk", "node_memory_Percpu", "node_memory_HardwareCorrupted",
			// vmstat
			"node_vmstat_nr_free_pages", "node_vmstat_nr_zone_inactive_anon", "node_vmstat_nr_zone_active_anon",
			"node_vmstat_nr_zone_inactive_file", "node_vmstat_nr_zone_active_file", "node_vmstat_nr_zone_unevictable",
			"node_vmstat_nr_zone_write_pending", "node_vmstat_nr_mlock", "node_vmstat_nr_page_table_pages",
			"node_vmstat_nr_kernel_stack", "node_vmstat_nr_bounce", "node_vmstat_nr_zspages", "node_vmstat_nr_free_cma",
			"node_vmstat_numa_hit", "node_vmstat_numa_miss", "node_vmstat_numa_foreign", "node_vmstat_numa_interleave",
			"node_vmstat_numa_local", "node_vmstat_numa_other", "node_vmstat_nr_inactive_anon", "node_vmstat_nr_active_anon",
			"node_vmstat_nr_inactive_file", "node_vmstat_nr_active_file", "node_vmstat_nr_unevictable",
			"node_vmstat_nr_slab_reclaimable", "node_vmstat_nr_slab_unreclaimable", "node_vmstat_nr_isolated_anon",
			"node_vmstat_nr_isolated_file", "node_vmstat_workingset_nodes", "node_vmstat_workingset_refault",
			"node_vmstat_workingset_activate", "node_vmstat_workingset_restore", "node_vmstat_workingset_nodereclaim",
			"node_vmstat_nr_file_pages", "node_vmstat_nr_writeback_temp", "node_vmstat_nr_shmem",
			"node_vmstat_nr_shmem_hugepages", "node_vmstat_nr_shmem_pmdmapped", "node_vmstat_nr_file_hugepages",
			"node_vmstat_nr_file_pmdmapped", "node_vmstat_nr_anon_transparent_hugepages", "node_vmstat_nr_unstable",
			"node_vmstat_nr_vmscan_write", "node_vmstat_nr_vmscan_immediate_reclaim", "node_vmstat_nr_dirtied",
			"node_vmstat_nr_written", "node_vmstat_nr_kernel_misc_reclaimable", "node_vmstat_nr_dirty_threshold",
			"node_vmstat_nr_dirty_background_threshold", "node_vmstat_pgalloc_dma", "node_vmstat_pgalloc_dma32",
			"node_vmstat_pgalloc_normal", "node_vmstat_pgalloc_movable", "node_vmstat_allocstall_dma",
			"node_vmstat_allocstall_dma32", "node_vmstat_allocstall_normal", "node_vmstat_allocstall_movable",
			"node_vmstat_pgskip_dma", "node_vmstat_pgskip_dma32", "node_vmstat_pgskip_normal",
			"node_vmstat_pgskip_movable", "node_vmstat_pgfree", "node_vmstat_pgactivate", "node_vmstat_pgdeactivate",
			"node_vmstat_pglazyfree", "node_vmstat_pgfault", "node_vmstat_pgmajfault", "node_vmstat_pglazyfreed",
			"node_vmstat_pgrefill", "node_vmstat_pgsteal_kswapd", "node_vmstat_pgsteal_direct",
			"node_vmstat_pgscan_kswapd", "node_vmstat_pgscan_direct", "node_vmstat_pgscan_direct_throttle",
			"node_vmstat_zone_reclaim_failed", "node_vmstat_pginodesteal", "node_vmstat_slabs_scanned",
			"node_vmstat_kswapd_inodesteal", "node_vmstat_kswapd_low_wmark_hit_quickly",
			"node_vmstat_kswapd_high_wmark_hit_quickly", "node_vmstat_pageoutrun", "node_vmstat_pgrotated",
			"node_vmstat_drop_pagecache", "node_vmstat_drop_slab", "node_vmstat_oom_kill", "node_vmstat_numa_pte_updates",
			"node_vmstat_numa_huge_pte_updates", "node_vmstat_numa_hint_faults", "node_vmstat_numa_hint_faults_local",
			"node_vmstat_numa_pages_migrated", "node_vmstat_pgmigrate_success", "node_vmstat_pgmigrate_fail",
			"node_vmstat_compact_migrate_scanned", "node_vmstat_compact_free_scanned", "node_vmstat_compact_isolated",
			"node_vmstat_compact_stall", "node_vmstat_compact_fail", "node_vmstat_compact_success",
			"node_vmstat_compact_daemon_wake", "node_vmstat_compact_daemon_migrate_scanned",
			"node_vmstat_compact_daemon_free_scanned", "node_vmstat_htlb_buddy_alloc_success",
			"node_vmstat_htlb_buddy_alloc_fail", "node_vmstat_unevictable_pgs_culled",
			"node_vmstat_unevictable_pgs_scanned", "node_vmstat_unevictable_pgs_rescued",
			"node_vmstat_unevictable_pgs_mlocked", "node_vmstat_unevictable_pgs_munlocked",
			"node_vmstat_unevictable_pgs_cleared", "node_vmstat_unevictable_pgs_stranded",
			"node_vmstat_thp_fault_alloc", "node_vmstat_thp_fault_fallback", "node_vmstat_thp_collapse_alloc",
			"node_vmstat_thp_collapse_alloc_failed", "node_vmstat_thp_file_alloc", "node_vmstat_thp_file_mapped",
			"node_vmstat_thp_split_page", "node_vmstat_thp_split_page_failed", "node_vmstat_thp_deferred_split_page",
			"node_vmstat_thp_split_pmd", "node_vmstat_thp_split_pud", "node_vmstat_thp_zero_page_alloc",
			"node_vmstat_thp_zero_page_alloc_failed", "node_vmstat_thp_swpout", "node_vmstat_thp_swpout_fallback",
			"node_vmstat_thp_migration_fail", "node_vmstat_workingset_restore_anon", "node_vmstat_workingset_activate_file",
			"node_vmstat_workingset_refault_file", "node_vmstat_thp_migration_split", "node_vmstat_workingset_refault_anon",
			"node_vmstat_pgreuse", "node_vmstat_thp_migration_success", "node_vmstat_workingset_activate_anon",
			"node_vmstat_workingset_restore_file", "node_vmstat_balloon_inflate", "node_vmstat_balloon_deflate",
			"node_vmstat_balloon_migrate", "node_vmstat_swap_ra", "node_vmstat_swap_ra_hit", "node_vmstat_nr_foll_pin_acquired",
			"node_vmstat_pgsteal_anon", "node_vmstat_pgsteal_file", "node_vmstat_pgscan_file", "node_vmstat_pgscan_anon",
			"node_vmstat_thp_file_fallback_charge", "node_vmstat_nr_foll_pin_released", "node_vmstat_thp_file_fallback",
			"node_vmstat_thp_fault_fallback_charge", "node_vmstat_nr_swapcached", "node_vmstat_direct_map_level2_splits",
			"node_vmstat_direct_map_level3_splits",
		},
		collector: NewMeminfoCollector,
	}

	pipeline(t, input)
}

func Test_getMeminfoStats(t *testing.T) {
	s, err := getMeminfoStats()
	assert.NoError(t, err)
	assert.Greater(t, len(s), 0)
}

func Test_parseMeminfoStats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/meminfo.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err := parseMeminfoStats(file)
	assert.NoError(t, err)

	want := map[string]float64{
		"MemTotal":          32839484 * 1024,
		"MemFree":           21570088 * 1024,
		"MemAvailable":      26190600 * 1024,
		"Buffers":           604064 * 1024,
		"Cached":            4361844 * 1024,
		"SwapCached":        0 * 1024,
		"Active":            7785324 * 1024,
		"Inactive":          2591484 * 1024,
		"Active(anon)":      5448748 * 1024,
		"Inactive(anon)":    344784 * 1024,
		"Active(file)":      2336576 * 1024,
		"Inactive(file)":    2246700 * 1024,
		"Unevictable":       0 * 1024,
		"Mlocked":           0 * 1024,
		"SwapTotal":         16777212 * 1024,
		"SwapFree":          16777212 * 1024,
		"Dirty":             36404 * 1024,
		"Writeback":         0 * 1024,
		"AnonPages":         5410948 * 1024,
		"Mapped":            1197820 * 1024,
		"Shmem":             386884 * 1024,
		"KReclaimable":      502080 * 1024,
		"Slab":              692516 * 1024,
		"SReclaimable":      502080 * 1024,
		"SUnreclaim":        190436 * 1024,
		"KernelStack":       16848 * 1024,
		"PageTables":        54472 * 1024,
		"NFS_Unstable":      0 * 1024,
		"Bounce":            0 * 1024,
		"WritebackTmp":      0 * 1024,
		"CommitLimit":       33196952 * 1024,
		"Committed_AS":      12808144 * 1024,
		"VmallocTotal":      34359738367 * 1024,
		"VmallocUsed":       34976 * 1024,
		"VmallocChunk":      0 * 1024,
		"Percpu":            6528 * 1024,
		"HardwareCorrupted": 0 * 1024,
		"AnonHugePages":     0 * 1024,
		"ShmemHugePages":    0 * 1024,
		"ShmemPmdMapped":    0 * 1024,
		"FileHugePages":     0 * 1024,
		"FilePmdMapped":     0 * 1024,
		"CmaTotal":          0 * 1024,
		"CmaFree":           0 * 1024,
		"HugePages_Total":   0 * 2048 * 1024,
		"HugePages_Free":    0 * 2048 * 1024,
		"HugePages_Rsvd":    0 * 2048 * 1024,
		"HugePages_Surp":    0 * 2048 * 1024,
		"Hugepagesize":      2048 * 1024,
		"Hugetlb":           0 * 1024,
		"DirectMap4k":       482128 * 1024,
		"DirectMap2M":       13101056 * 1024,
		"DirectMap1G":       19922944 * 1024,
	}

	assert.Equal(t, want, stats)

	// test with wrong format file
	file, err = os.Open(filepath.Clean("testdata/proc/netdev.golden"))
	assert.NoError(t, err)
	defer func() { _ = file.Close() }()

	stats, err = parseMeminfoStats(file)
	assert.Error(t, err)
	assert.Nil(t, stats)
}

func Test_getVmstatStats(t *testing.T) {
	s, err := getVmstatStats()
	assert.NoError(t, err)
	assert.Greater(t, len(s), 0)
}

func Test_parseVmstatStats(t *testing.T) {
	file, err := os.Open(filepath.Clean("testdata/proc/vmstat.golden"))
	assert.NoError(t, err)

	stats, err := parseVmstatStats(file)
	assert.NoError(t, err)

	wantStats := map[string]float64{
		"oom_kill":            10,
		"nr_zone_active_file": 1933629,
		"nr_unevictable":      24,
		"nr_writeback":        0,
		"pgactivate":          57995375,
	}

	for k, want := range wantStats {
		if got, ok := stats[k]; ok {
			assert.Equal(t, want, got)
		} else {
			assert.Fail(t, "not found")
		}
	}

	assert.NoError(t, file.Close())

	// test with invalid values
	file, err = os.Open(filepath.Clean("testdata/proc/vmstat.invalid.1"))
	assert.NoError(t, err)
	stats, err = parseVmstatStats(file)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(stats))
	assert.NoError(t, file.Close())

	// test with wrong number of fields
	file, err = os.Open(filepath.Clean("testdata/proc/vmstat.invalid.2"))
	assert.NoError(t, err)
	_, err = parseVmstatStats(file)
	assert.Error(t, err)
	assert.NoError(t, file.Close())

	// test with wrong format file
	file, err = os.Open(filepath.Clean("testdata/proc/netdev.golden"))
	assert.NoError(t, err)

	stats, err = parseVmstatStats(file)
	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.NoError(t, file.Close())
}

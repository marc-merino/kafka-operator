//! Builders that assemble Kubernetes resources for kafka rolegroups.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{kvp::Labels, v2::kvp::label};

use crate::{
    controller::{
        CONTROLLER_NAME, KubernetesResources, OPERATOR_NAME, PRODUCT_NAME, Prepared, RoleGroupName,
        RoleName, ValidatedCluster,
        build::{
            properties::{
                listener::get_kafka_listener_config, product_logging::vector_config_file_content,
            },
            resource::{
                config_map::build_rolegroup_config_map,
                discovery::build_discovery_configmap,
                listener::build_broker_rolegroup_bootstrap_listener,
                pdb::build_pdb,
                rbac::{build_role_binding, build_service_account},
                service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
                statefulset::{
                    build_broker_rolegroup_statefulset, build_controller_rolegroup_statefulset,
                },
            },
        },
    },
    crd::role::{AnyConfig, KafkaRole},
};

pub mod command;
pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod properties;
pub mod resource;
pub mod security;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    ConfigMap {
        source: resource::config_map::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {role_group}"))]
    StatefulSet {
        source: resource::statefulset::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    DiscoveryConfigMap { source: resource::discovery::Error },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every external reference is already dereferenced and
/// validated by this point, so the only errors are resource-assembly failures.
///
/// This includes the discovery `ConfigMap`, built from the bootstrap `Listener`s fetched in the
/// dereference step; see
/// [`build_discovery_configmap`] for how its
/// content depends on their ingress addresses.
pub fn build(cluster: &ValidatedCluster) -> Result<KubernetesResources<Prepared>, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut listeners = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    for (role, role_group_configs) in &cluster.role_group_configs {
        // Kafka's `GenericRoleConfig` only carries the PodDisruptionBudget.
        if let Some(role_config) = cluster.role_configs.get(role) {
            pod_disruption_budgets.extend(build_pdb(&role_config.pdb, cluster, role));
        }

        for (role_group_name, validated_rg) in role_group_configs {
            // The Vector agent config is the static `vector.yaml`, added to the rolegroup
            // ConfigMap only when the Vector agent is enabled (resolved during validation).
            let vector_config = validated_rg
                .config
                .logging
                .vector_container
                .is_some()
                .then(vector_config_file_content);

            services.push(build_rolegroup_headless_service(
                cluster,
                role,
                role_group_name,
                &cluster.cluster_config.kafka_security,
            ));
            services.push(build_rolegroup_metrics_service(
                cluster,
                role,
                role_group_name,
            ));

            let kafka_listeners = get_kafka_listener_config(
                cluster,
                &cluster.cluster_config.kafka_security,
                role,
                role_group_name,
            );

            config_maps.push(
                build_rolegroup_config_map(
                    cluster,
                    role_group_name,
                    validated_rg,
                    &kafka_listeners,
                    vector_config,
                )
                .context(ConfigMapSnafu {
                    role_group: role_group_name.clone(),
                })?,
            );

            let stateful_set = match role {
                KafkaRole::Broker => {
                    build_broker_rolegroup_statefulset(role, role_group_name, cluster, validated_rg)
                }
                KafkaRole::Controller => build_controller_rolegroup_statefulset(
                    role,
                    role_group_name,
                    cluster,
                    validated_rg,
                ),
            }
            .context(StatefulSetSnafu {
                role_group: role_group_name.clone(),
            })?;
            stateful_sets.push(stateful_set);

            // Only broker role groups get a bootstrap Listener.
            if let AnyConfig::Broker(broker_config) = &validated_rg.config.config {
                listeners.push(build_broker_rolegroup_bootstrap_listener(
                    cluster,
                    role,
                    role_group_name,
                    broker_config,
                ));
            }
        }
    }

    config_maps.push(build_discovery_configmap(cluster).context(DiscoveryConfigMapSnafu)?);

    Ok(KubernetesResources {
        stateful_sets,
        services,
        listeners,
        config_maps,
        pod_disruption_budgets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
        status: PhantomData,
    })
}

pub(crate) fn recommended_labels_for_cluster_resources(cluster: &ValidatedCluster) -> Labels {
    label::recommended_labels_for_cluster_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
    )
}

pub(crate) fn recommended_labels_for_role_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
) -> Labels {
    label::recommended_labels_for_role_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
    )
}

pub(crate) fn recommended_labels_for_role_group_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels_for_role_group_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
        role_group_name,
    )
}

pub(crate) fn recommended_labels_for_unversioned_role_group_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels_for_unversioned_role_group_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
        role_group_name,
    )
}

/// Selector labels matching the pods of a role group.
pub(crate) fn role_group_selector(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::role_group_selector(&cluster.name, &PRODUCT_NAME, role_name, role_group_name)
}

#[cfg(test)]
mod tests {
    use stackable_operator::kube::Resource;

    use super::{build, security::STACKABLE_TLS_KAFKA_INTERNAL_DIR};
    use crate::{
        controller::{
            ValidatedCluster,
            test_support::{
                bootstrap_listener, ingress_address, minimal_kafka, validated_cluster,
                zookeeper_mode_cluster,
            },
        },
        crd::{STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR},
    };

    /// Sorted `metadata.name`s of the given resources, for order-independent assertions.
    fn sorted_names(resources: &[impl Resource]) -> Vec<&str> {
        let mut names: Vec<&str> = resources
            .iter()
            .filter_map(|resource| resource.meta().name.as_deref())
            .collect();
        names.sort();
        names
    }

    /// A KRaft cluster with one `broker` and one `controller` role group, resolved through the real
    /// validate step (mirroring the other build fixtures), since [`ValidatedCluster`] carries
    /// several resolved types that are impractical to construct by hand.
    fn kraft_mode_cluster() -> ValidatedCluster {
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                metadataManager: kraft
              controllers:
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        validated_cluster(&kafka)
    }

    #[rstest::rstest]
    #[case::zookeeper(false)]
    #[case::kraft(true)]
    fn jmx_config_is_supplied_by_the_mounted_rolegroup_configmap(#[case] kraft: bool) {
        let cluster = if kraft {
            kraft_mode_cluster()
        } else {
            zookeeper_mode_cluster()
        };
        let resources = build(&cluster).expect("build succeeds");
        assert_eq!(resources.stateful_sets.len(), if kraft { 2 } else { 1 });
        // Only the existing role-group ConfigMaps and discovery ConfigMap are needed.
        assert_eq!(
            resources.config_maps.len(),
            resources.stateful_sets.len() + 1
        );

        for sts in &resources.stateful_sets {
            let pod = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
            let kafka = pod.containers.iter().find(|c| c.name == "kafka").unwrap();
            let extra_args = kafka
                .env
                .as_ref()
                .unwrap()
                .iter()
                .find(|env| env.name == "EXTRA_ARGS")
                .unwrap()
                .value
                .as_ref()
                .unwrap();
            let agent_args: Vec<_> = extra_args
                .split_whitespace()
                .filter(|arg| arg.starts_with("-javaagent:"))
                .collect();
            assert_eq!(
                agent_args,
                [
                    "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9606:/stackable/config/jmx-exporter.yaml"
                ]
            );
            let yaml_path = agent_args[0].rsplit_once(':').unwrap().1;
            let mount = kafka
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .find(|mount| mount.mount_path == STACKABLE_CONFIG_DIR)
                .unwrap();
            assert_eq!(mount.name, "config");
            assert!(mount.sub_path.is_none());
            let volume = pod
                .volumes
                .as_ref()
                .unwrap()
                .iter()
                .find(|volume| volume.name == mount.name)
                .unwrap();
            let source = volume.config_map.as_ref().unwrap();
            assert_eq!(Some(&source.name), sts.metadata.name.as_ref());
            assert!(
                source.items.is_none(),
                "all ConfigMap keys must be projected"
            );
            let cm = resources
                .config_maps
                .iter()
                .find(|cm| cm.metadata.name.as_ref() == Some(&source.name))
                .unwrap();
            let filename = yaml_path
                .strip_prefix(&format!("{}/", mount.mount_path))
                .unwrap();
            let emitted = &cm.data.as_ref().unwrap()[filename];
            assert_eq!(emitted, include_str!("properties/jmx/server.yaml"));
            let yaml: serde_yaml::Value = serde_yaml::from_str(emitted).unwrap();
            assert!(!yaml["rules"].as_sequence().unwrap().is_empty());

            // No command, init container or environment variable may fall back to image YAML.
            let serialized = serde_json::to_string(sts).unwrap();
            assert!(!serialized.contains("/stackable/jmx/server.yaml"));
            assert!(!serialized.contains("/stackable/jmx/broker.yaml"));
        }
    }

    #[test]
    fn jmx_agent_overrides_are_honored_by_broker_and_controller_statefulsets() {
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                metadataManager: kraft
              brokers:
                jvmArgumentOverrides:
                  add: ["-Dexample.setting=role"]
                roleGroups:
                  default:
                    replicas: 1
                    jvmArgumentOverrides:
                      removeRegex: ["-javaagent:.*", "-Dexample.setting=.*"]
                      add: ["-Dexample.setting=group"]
              controllers:
                jvmArgumentOverrides:
                  add: ["-Dexample.setting=role"]
                roleGroups:
                  default:
                    replicas: 1
                    jvmArgumentOverrides:
                      removeRegex: ["-javaagent:.*", "-Dexample.setting=.*"]
                      add: ["-Dexample.setting=group"]
        "#,
        );
        let resources = build(&validated_cluster(&kafka)).unwrap();
        assert_eq!(resources.stateful_sets.len(), 2);
        for sts in resources.stateful_sets {
            let pod = sts.spec.unwrap().template.spec.unwrap();
            let kafka = pod.containers.iter().find(|c| c.name == "kafka").unwrap();
            let env = kafka.env.as_ref().unwrap();
            let extra_args = env
                .iter()
                .find(|env| env.name == "EXTRA_ARGS")
                .unwrap()
                .value
                .as_ref()
                .unwrap();
            assert!(!extra_args.contains("-javaagent:"));
            assert!(!extra_args.contains("-Dexample.setting=role"));
            assert!(extra_args.contains("-Dexample.setting=group"));
            assert!(
                extra_args
                    .contains("-Djava.security.properties=/stackable/config/security.properties")
            );
            assert!(
                env.iter()
                    .find(|env| env.name == "KAFKA_HEAP_OPTS")
                    .unwrap()
                    .value
                    .as_ref()
                    .unwrap()
                    .contains("-Xmx")
            );
        }
    }

    #[test]
    fn build_succeeds_when_every_kraft_role_group_is_scaled_to_zero() {
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                metadataManager: kraft
              controllers:
                roleGroups:
                  default:
                    replicas: 0
              brokers:
                roleGroups:
                  default:
                    replicas: 0
            "#,
        );
        let cluster = validated_cluster(&kafka);

        build(&cluster).expect("build succeeds when the whole KRaft cluster is stopped");
    }

    #[test]
    fn build_produces_expected_resource_names() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        // One StatefulSet per role group.
        assert_eq!(
            sorted_names(&resources.stateful_sets),
            [
                "simple-kafka-broker-default",
                "simple-kafka-controller-default"
            ]
        );
        // One rolegroup ConfigMap per role group, plus the discovery ConfigMap (named after the
        // cluster), which is written even while no bootstrap Listener has an address yet.
        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "simple-kafka",
                "simple-kafka-broker-default",
                "simple-kafka-controller-default"
            ]
        );
        // One headless and one metrics Service per role group.
        assert_eq!(
            sorted_names(&resources.services),
            [
                "simple-kafka-broker-default-headless",
                "simple-kafka-broker-default-metrics",
                "simple-kafka-controller-default-headless",
                "simple-kafka-controller-default-metrics",
            ]
        );
        // Only broker role groups get a bootstrap Listener.
        assert_eq!(
            sorted_names(&resources.listeners),
            ["simple-kafka-broker-default-bootstrap"]
        );
        // A default PodDisruptionBudget per role.
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["simple-kafka-broker", "simple-kafka-controller"]
        );
        // The cluster-shared RBAC pair.
        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["simple-kafka-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["simple-kafka-rolebinding"]
        );
    }

    #[test]
    fn build_writes_listener_addresses_to_the_discovery_configmap() {
        let mut cluster = kraft_mode_cluster();
        let port_name = cluster
            .cluster_config
            .kafka_security
            .client_port_name()
            .to_owned();
        cluster.bootstrap_listeners = vec![bootstrap_listener(Some(vec![ingress_address(
            "host1", &port_name, 9093,
        )]))];

        let resources = build(&cluster).expect("build succeeds");

        let discovery_cm = resources
            .config_maps
            .iter()
            .find(|config_map| config_map.metadata.name.as_deref() == Some("simple-kafka"))
            .expect("the discovery ConfigMap should be built");
        assert_eq!(
            discovery_cm
                .data
                .as_ref()
                .expect("the discovery ConfigMap should carry data")
                .get("KAFKA")
                .map(String::as_str),
            Some("host1:9093")
        );
    }

    #[test]
    fn quorum_manager_sidecar_mounts_every_directory_referenced_by_admin_client_properties() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        let controller_sts = resources
            .stateful_sets
            .iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-controller-default"))
            .expect("the controller StatefulSet should be built");
        let pod_spec = controller_sts
            .spec
            .as_ref()
            .expect("the StatefulSet should have a spec")
            .template
            .spec
            .as_ref()
            .expect("the pod template should have a spec");
        let quorum_manager = pod_spec
            .containers
            .iter()
            .find(|c| c.name == "quorum-manager")
            .expect("the controller pod should have a quorum-manager sidecar");

        let mount_paths: Vec<&str> = quorum_manager
            .volume_mounts
            .as_ref()
            .expect("the sidecar should have volume mounts")
            .iter()
            .map(|vm| vm.mount_path.as_str())
            .collect();
        assert!(
            mount_paths.contains(&STACKABLE_CONFIG_DIR),
            "the sidecar must mount the config directory carrying admin-client.properties, got: {mount_paths:?}"
        );
        assert!(
            mount_paths.contains(&STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            "the sidecar must mount the internal TLS directory admin-client.properties points its keystore/truststore at, got: {mount_paths:?}"
        );
        // `add-controller` reads this controller's own on-disk `meta.properties` (written by
        // `kafka-storage.sh format`, and pointed at by `log.dirs` in the merged config it
        // connects with) to build the voter registration payload. Without this mount the path
        // doesn't exist in the sidecar and every attempt fails with "Unable to read
        // meta.properties from /stackable/data/kraft".
        assert!(
            mount_paths.contains(&STACKABLE_DATA_DIR),
            "the sidecar must mount the data directory holding its own meta.properties, or add-controller can never read its own identity, got: {mount_paths:?}"
        );
    }

    /// ZooKeeper mode has no `controller` role, so `build()` emits no controller resources while
    /// still producing the broker's bootstrap Listener.
    #[test]
    fn build_zookeeper_mode_has_no_controller_resources() {
        let cluster = zookeeper_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.stateful_sets),
            ["simple-kafka-broker-default"]
        );
        assert_eq!(
            sorted_names(&resources.services),
            [
                "simple-kafka-broker-default-headless",
                "simple-kafka-broker-default-metrics",
            ]
        );
        assert_eq!(
            sorted_names(&resources.listeners),
            ["simple-kafka-broker-default-bootstrap"]
        );
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["simple-kafka-broker"]
        );
    }
}

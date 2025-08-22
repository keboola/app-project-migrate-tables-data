<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->enumNode('mode')->values(['sapi', 'database'])->defaultValue('sapi')->end()
                ->booleanNode('dryRun')->defaultFalse()->end()
                ->booleanNode('isSourceByodb')->defaultFalse()->end()
                ->scalarNode('sourceByodb')->end()
                ->scalarNode('sourceKbcUrl')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('#sourceKbcToken')->isRequired()->cannotBeEmpty()->end()
                ->arrayNode('includeWorkspaceSchemas')->prototype('scalar')->end()->end()
                ->booleanNode('preserveTimestamp')->defaultFalse()->end()
                ->arrayNode('tables')->prototype('scalar')->end()->end()
                ->booleanNode('migrateData')->defaultTrue()->end()
                ->arrayNode('replica')
                    ->children()
                        ->booleanNode('create')->defaultTrue()->end()
                        ->booleanNode('refresh')->defaultTrue()->end()
                        ->booleanNode('drop')->defaultTrue()->end()
                    ->end()
                ->end()
                ->arrayNode('db')
                    ->children()
                        ->scalarNode('host')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('username')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('#password')->defaultValue('')->end()
                        ->scalarNode('#privateKey')->end()
                        ->scalarNode('warehouse')->isRequired()->cannotBeEmpty()->end()
                        ->enumNode('warehouse_size')->values(['SMALL', 'MEDIUM', 'LARGE'])->defaultValue('SMALL')->end()
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}

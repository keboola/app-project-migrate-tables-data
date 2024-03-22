<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->validate()->always(function ($v) {
                if ($v['mode'] === 'sapi') {
                    if (empty($v['#sourceKbcToken'])) {
                        throw new InvalidConfigurationException('sourceKbcToken must be set when mode is sapi');
                    }
                }
                if ($v['mode'] === 'database') {
                    if (empty($v['sourceDatabase'])) {
                        throw new InvalidConfigurationException('sourceDatabase must be set when mode is database');
                    }
                    if (empty($v['targetDatabase'])) {
                        throw new InvalidConfigurationException('targetDatabase must be set when mode is database');
                    }
                }
                return $v;
            })->end()
            ->children()
                ->enumNode('mode')->values(['sapi', 'database'])->defaultValue('sapi')->end()
                ->scalarNode('sourceKbcUrl')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('#sourceKbcToken')->end()
                ->arrayNode('tables')->prototype('scalar')->end()->end()
                ->scalarNode('sourceDatabase')->end()
                ->scalarNode('targetDatabase')->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}

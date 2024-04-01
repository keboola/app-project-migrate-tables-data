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
                ->scalarNode('sourceKbcUrl')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('#sourceKbcToken')->isRequired()->cannotBeEmpty()->end()
                ->arrayNode('tables')->prototype('scalar')->end()->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
